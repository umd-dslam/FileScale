package org.apache.hadoop.hdfs.nnproxy.server.mount;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import dnl.utils.text.table.TextTable;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.nnproxy.ProxyConfig;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages mount table and keep up-to-date to VoltDB's ZooKeeper. */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MountsManager extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(MountsManager.class);

  static class MountEntry {
    public final String fsUri;
    public final String mountPoint;
    public final String[] attributes;

    public MountEntry(String fsUri, String mountPoint, String[] attributes) {
      this.fsUri = fsUri;
      this.mountPoint = mountPoint;
      this.attributes = attributes;
    }

    @Override
    public String toString() {
      return "MountEntry ["
          + "fsUri="
          + fsUri
          + ", mountPoint="
          + mountPoint
          + ", attributes="
          + Arrays.toString(attributes)
          + ']';
    }
  }

  CuratorFramework framework;
  String zkMountTablePath;
  ImmutableList<MountEntry> mounts;
  ImmutableList<String> allFs;
  MountEntry root;
  NodeCache nodeCache;
  Map<String, List<MountEntry>> lookupMap;
  Random rand;

  @VisibleForTesting protected volatile boolean installed;

  public MountsManager() {
    super("MountsManager");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String zkConnectString = conf.get(ProxyConfig.MOUNT_TABLE_ZK_QUORUM);
    zkMountTablePath = conf.get(ProxyConfig.MOUNT_TABLE_ZK_PATH);
    int sessionTimeout =
        conf.getInt(
            ProxyConfig.MOUNT_TABLE_ZK_SESSION_TIMEOUT,
            ProxyConfig.MOUNT_TABLE_ZK_SESSION_TIMEOUT_DEFAULT);
    int connectionTimeout =
        conf.getInt(
            ProxyConfig.MOUNT_TABLE_ZK_CONNECTION_TIMEOUT,
            ProxyConfig.MOUNT_TABLE_ZK_CONNECTION_TIMEOUT_DEFAULT);
    int maxRetries =
        conf.getInt(
            ProxyConfig.MOUNT_TABLE_ZK_MAX_RETRIES, ProxyConfig.MOUNT_TABLE_ZK_MAX_RETRIES_DEFAULT);
    int retryBaseSleep =
        conf.getInt(
            ProxyConfig.MOUNT_TABLE_ZK_RETRY_BASE_SLEEP,
            ProxyConfig.MOUNT_TABLE_ZK_RETRY_BASE_SLEEP_DEFAULT);
    framework =
        CuratorFrameworkFactory.newClient(
            zkConnectString,
            sessionTimeout,
            connectionTimeout,
            new ExponentialBackoffRetry(retryBaseSleep, maxRetries));
    rand = new Random();
    installed = false;
  }

  public ImmutableList<MountEntry> getMounts() {
    return mounts;
  }

  public ImmutableList<String> getAllFs() {
    return allFs;
  }

  public String resolve(String path) {
    ImmutableList<MountEntry> entries = this.mounts;
    MountEntry chosen = null;
    for (MountEntry entry : entries) {
      if (path == null
          || !(path.startsWith(entry.mountPoint + "/") || path.equals(entry.mountPoint))) {
        continue;
      }
      if (chosen == null || chosen.mountPoint.length() < entry.mountPoint.length()) {
        chosen = entry;
      }
    }
    if (chosen == null) {
      chosen = root;
    }
    return chosen.fsUri;
  }

  public String resolveOpt(String path) {
    MountEntry chosen = null;
    if (path == null) {
      chosen = root;
    } else {
      chosen = resolveParentPath(path, path);
      if (chosen == null) {
        StringBuilder seg = new StringBuilder(path.length());
        seg.append(path);
        for (int i = path.length() - 1; i >= 0; i--) {
          if (path.charAt(i) == '/') {
            seg.setLength(i);
            MountEntry entry = resolveParentPath(seg.toString(), path);
            if (entry != null) {
              chosen = entry;
              break;
            }
          }
        }
      }
    }
    if (chosen == null) {
      chosen = root;
    }
    return chosen.fsUri;
  }

  private MountEntry resolveParentPath(String parent, String path) {
    Map<String, List<MountEntry>> entries = this.lookupMap;
    List<MountEntry> mounts = entries.get(parent);
    if (mounts == null) {
      LOG.debug("resolve not found");
      return null;
    }
    return mounts.get(rand.nextInt(mounts.size()));
  }

  /**
   * Determine whether given path is exactly a valid mount point
   *
   * @param path
   * @return
   */
  public boolean isMountPoint(String path) {
    ImmutableList<MountEntry> entries = this.mounts;
    for (MountEntry entry : entries) {
      if (entry.mountPoint.equals(path)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine whether given path contains a mount point. Directory is considered unified even if
   * itself is a mount point, unless it contains another mount point.
   *
   * @param path
   * @return
   */
  public boolean isUnified(String path) {
    String prefix = path + "/";
    ImmutableList<MountEntry> entries = this.mounts;
    for (MountEntry entry : entries) {
      if (entry.mountPoint.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

  protected void installMountTable(List<MountEntry> entries) {
    LOG.info("Installed mount table: " + entries);
    List<String> fs = new ArrayList<>();
    for (MountEntry entry : entries) {
      if (entry.mountPoint.equals("/")) {
        root = entry;
      }
      if (!fs.contains(entry.fsUri)) {
        fs.add(entry.fsUri);
      }
    }
    this.allFs = ImmutableList.copyOf(fs);
    this.mounts = ImmutableList.copyOf(entries);
    this.lookupMap = buildLookupMap(entries);
    this.installed = true;
  }

  protected List<MountEntry> parseMountTable(String mounts) {
    List<MountEntry> table = new ArrayList<>();
    boolean hasRoot = false;
    for (String s : mounts.split("\n")) {
      if (StringUtils.isEmpty(s)) {
        continue;
      }
      String[] cols = s.split(" ");
      String fsUri = cols[0];
      String mountPoint = cols[1];
      String[] attrs = (cols.length > 2) ? cols[2].split(",") : new String[0];
      table.add(new MountEntry(fsUri, mountPoint, attrs));
      if (mountPoint.equals("/")) {
        hasRoot = true;
      }
    }
    if (!hasRoot) {
      LOG.error("Ignored invalid mount table: " + mounts);
      return null;
    }
    return table;
  }

  protected void handleMountTableChange(byte[] data) {
    if (data == null || data.length == 0) {
      LOG.info("Invalid mount table");
      return;
    }
    String mounts = new String(data);
    List<MountEntry> table = parseMountTable(mounts);
    if (table != null) {
      installMountTable(table);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    framework.start();
    nodeCache = new NodeCache(framework, zkMountTablePath, false);
    nodeCache
        .getListenable()
        .addListener(
            new NodeCacheListener() {
              @Override
              public void nodeChanged() throws Exception {
                handleMountTableChange(nodeCache.getCurrentData().getData());
              }
            });
    nodeCache.start(false);
  }

  @Override
  protected void serviceStop() throws Exception {
    nodeCache.close();
    framework.close();
  }

  public void waitUntilInstalled() throws InterruptedException {
    while (!installed) {
      Thread.sleep(100);
    }
  }

  public void dump() {
    ImmutableList<MountEntry> entries = this.mounts;
    StringBuilder result = new StringBuilder();
    System.out.println("\t\t\t============================================");
    System.out.println("\t\t\t               Mount Table                  ");
    System.out.println("\t\t\t============================================");
    String[] columnNames = {"NameNode", "Path", "Attributes"};
    Object[][] tuples = new Object[entries.size()][];
    int i = 0;
    for (MountEntry entry : entries) {
      tuples[i++] =
          new Object[] {entry.fsUri, entry.mountPoint, StringUtils.join(entry.attributes, ",")};
    }
    TextTable tt = new TextTable(columnNames, tuples);
    // this adds the numbering on the left
    tt.setAddRowNumbering(true);
    // sort by the first column
    tt.setSort(0);
    tt.printTable();
  }

  public void load(String mounts) throws Exception {
    if (framework.checkExists().forPath(zkMountTablePath) == null) {
      framework.create().forPath(zkMountTablePath, mounts.getBytes());
    } else {
      framework.setData().forPath(zkMountTablePath, mounts.getBytes());
    }
  }

  protected Map<String, List<MountEntry>> buildLookupMap(List<MountEntry> entries) {
    Map<String, List<MountEntry>> lookupMap = new HashMap<>();
    for (MountEntry entry : entries) {
      List<MountEntry> mounts = lookupMap.get(entry.mountPoint);
      if (mounts == null) {
        mounts = new ArrayList<>();
        lookupMap.put(entry.mountPoint, mounts);
      }
      mounts.add(entry);
      if (entry.mountPoint.equals("/")) {
        lookupMap.put("", mounts);
      }
    }
    return lookupMap;
  }
}
