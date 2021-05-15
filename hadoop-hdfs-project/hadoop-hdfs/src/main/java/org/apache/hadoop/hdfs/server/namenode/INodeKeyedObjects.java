package org.apache.hadoop.hdfs.server.namenode;

import static java.util.concurrent.TimeUnit.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.db.Database;
import org.apache.hadoop.hdfs.db.DatabaseINode;
import org.apache.hadoop.hdfs.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

public class INodeKeyedObjects {
  private static IndexedCache<String, INode> cache;
  private static Cache<String, String> move;

  private static Set<String> concurrentUpdateSet;
  private static Set<String> concurrentRenameSet;
  private static Set<Long> concurrentRemoveSet;
  private static long preRemoveSize = 0;
  private static long preRenameSize = 0;
  private static long preUpdateSize = 0;

  // gloabal unique ID (VoltDB)
  private static AtomicLong uniqueId = new AtomicLong();

  private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  static final Logger LOG = LoggerFactory.getLogger(INodeKeyedObjects.class);

  INodeKeyedObjects() {}

  public static long getUniqueId() {
    return uniqueId.longValue();
  }

  public static void setUniqueId(long id) {
    uniqueId.set(id);
  }

  public static Set<String> getUpdateSet() {
    if (concurrentUpdateSet == null) {
      ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
      concurrentUpdateSet = map.newKeySet();
    }
    return concurrentUpdateSet;
  }

  public static Set<Long> getRemoveSet() {
    if (concurrentRemoveSet == null) {
      ConcurrentHashMap<Long, Integer> map = new ConcurrentHashMap<>();
      concurrentRemoveSet = map.newKeySet();
    }
    return concurrentRemoveSet;
  }

  public static Set<String> getRenameSet() {
    if (concurrentRenameSet == null) {
      ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
      concurrentRenameSet = map.newKeySet();
    }
    return concurrentRenameSet;
  }

  private static void insertToDB() {
    if (concurrentUpdateSet == null) {
      return;
    }
    int i = 0;
    final int num = 1024;
    long updateSize = concurrentUpdateSet.size();
    String env = System.getenv("DATABASE");
    DatabaseConnection conn = Database.getInstance().getConnection();
    if (updateSize >= num) {
      Iterator<String> iterator = concurrentUpdateSet.iterator();
      if (LOG.isInfoEnabled()) {
        LOG.info("Sync update files/directories from cache to database.");
      }

      List<Long> longAttr = new ArrayList<>();
      List<String> strAttr = new ArrayList<>();

      List<Long> fileIds = new ArrayList<>();
      List<String> fileAttr = new ArrayList<>();
      Map<BinaryObject, BinaryObject> map = new TreeMap<>();
      while (iterator.hasNext()) {
        INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
        if (inode == null) continue;
        if (env.equals("VOLT")) {
          strAttr.add(inode.getLocalName());
          if (inode.getId() == 16385) {
            strAttr.add(" ");
          } else {
            strAttr.add(inode.getParentName());
          }
          longAttr.add(inode.getParentId());
          longAttr.add(inode.getId());
          longAttr.add(inode.getModificationTime());
          longAttr.add(inode.getAccessTime());
          longAttr.add(inode.getPermissionLong());
          if (inode.isDirectory()) {
            longAttr.add(0L);
          } else {
            longAttr.add(inode.asFile().getHeaderLong());
            FileUnderConstructionFeature uc = inode.asFile().getFileUnderConstructionFeature();
            if (uc != null) {
              fileIds.add(inode.getId());
              fileAttr.add(uc.getClientName(inode.getId()));
              fileAttr.add(uc.getClientMachine(inode.getId()));
            }
          }
        } else if (env.equals("IGNITE")) {
          BinaryObjectBuilder inodeKeyBuilder = conn.getIgniteClient().binary().builder("InodeKey");
          BinaryObject inodeKey = inodeKeyBuilder.setField("parentName", inode.getParentName()).setField("name", inode.getLocalName()).build();
          BinaryObjectBuilder inodeBuilder = conn.getIgniteClient().binary().builder("INode");
          long header = 0L;
          if (inode.isFile()) {
            header = inode.asFile().getHeaderLong();
          } 
          String parentName = " ";
          if (inode.getId() != 16385) {
            parentName = inode.getParentName();
          }
          BinaryObject inodeValue = inodeBuilder
            .setField("id", inode.getId(), Long.class)
            .setField("parent", inode.getParentId(), Long.class)
            .setField("parentName", parentName)
            .setField("name", inode.getLocalName())
            .setField("accessTime", inode.getAccessTime(), Long.class)
            .setField("modificationTime", inode.getModificationTime(), Long.class)
            .setField("header", header, Long.class)
            .setField("permission", inode.getPermissionLong(), Long.class)
            .build();
          map.put(inodeKey, inodeValue);
        }
        iterator.remove();
        if (++i >= num) break;
      }
      try {
        if (env.equals("VOLT") && strAttr.size() > 0) {          
          INodeKeyedObjects.setUniqueId(DatabaseINode.batchUpdateINodes(longAttr, strAttr, fileIds, fileAttr));
        } else if (env.equals("IGNITE") && map.size() > 0) {
          IgniteCache<BinaryObject, BinaryObject> inodesBinary = conn.getIgniteClient().cache("inodes").withKeepBinary();
          inodesBinary.putAll(map);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (updateSize > 0 && preUpdateSize == updateSize) {
        Iterator<String> iterator = concurrentUpdateSet.iterator();
        if (LOG.isInfoEnabled()) {
          LOG.info("Propagate updated files/directories from cache to database.");
        }
        try {
          List<Long> longAttr = new ArrayList<>();
          List<String> strAttr = new ArrayList<>();
          List<Long> fileIds = new ArrayList<>();
          List<String> fileAttr = new ArrayList<>();
          Map<BinaryObject, BinaryObject> map = new TreeMap<>();
          while (iterator.hasNext()) {
            INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
            if (inode == null) continue;
            if (env.equals("VOLT")) {
              strAttr.add(inode.getLocalName());
              if (inode.getId() == 16385) {
                strAttr.add(" ");
              } else {
                strAttr.add(inode.getParentName());
              }
              longAttr.add(inode.getParentId());
              longAttr.add(inode.getId());
              longAttr.add(inode.getModificationTime());
              longAttr.add(inode.getAccessTime());
              longAttr.add(inode.getPermissionLong());
              if (inode.isDirectory()) {
                longAttr.add(0L);
              } else {
                longAttr.add(inode.asFile().getHeaderLong());
                FileUnderConstructionFeature uc = inode.asFile().getFileUnderConstructionFeature();
                if (uc != null) {
                  fileIds.add(inode.getId());
                  fileAttr.add(uc.getClientName(inode.getId()));
                  fileAttr.add(uc.getClientMachine(inode.getId()));
                }
              }
            } else if (env.equals("IGNITE")) {
              BinaryObjectBuilder inodeKeyBuilder = conn.getIgniteClient().binary().builder("InodeKey");
              BinaryObject inodeKey = inodeKeyBuilder.setField("parentName", inode.getParentName()).setField("name", inode.getLocalName()).build();
              BinaryObjectBuilder inodeBuilder = conn.getIgniteClient().binary().builder("INode");
              long header = 0L;
              if (inode.isFile()) {
                header = inode.asFile().getHeaderLong();
              } 
              String parentName = " ";
              if (inode.getId() != 16385) {
                parentName = inode.getParentName();
              }
              BinaryObject inodeValue = inodeBuilder
                .setField("id", inode.getId(), Long.class)
                .setField("parent", inode.getParentId(), Long.class)
                .setField("parentName", parentName)
                .setField("name", inode.getLocalName())
                .setField("accessTime", inode.getAccessTime(), Long.class)
                .setField("modificationTime", inode.getModificationTime(), Long.class)
                .setField("header", header, Long.class)
                .setField("permission", inode.getPermissionLong(), Long.class)
                .build();
              map.put(inodeKey, inodeValue);             
            }
            iterator.remove();             
          }
          if (env.equals("VOLT") && strAttr.size() > 0) {          
            INodeKeyedObjects.setUniqueId(DatabaseINode.batchUpdateINodes(longAttr, strAttr, fileIds, fileAttr));
          } else if (env.equals("IGNITE") && map.size() > 0) {
            IgniteCache<BinaryObject, BinaryObject> inodesBinary = conn.getIgniteClient().cache("inodes").withKeepBinary();
            inodesBinary.putAll(map);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    preUpdateSize = concurrentUpdateSet.size();
    Database.getInstance().retConnection(conn);
  }

  private static void removeToDB() {
    if (concurrentRemoveSet == null) {
      return;
    }
    int i = 0;
    final int num = 1024;
    List<Long> removeIds = new ArrayList<>();
    Set<BinaryObject> removeKeys = new HashSet<>();
    long removeSize = concurrentRemoveSet.size();
    String env = System.getenv("DATABASE");
    DatabaseConnection conn = Database.getInstance().getConnection();
    if (removeSize >= num) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Propagate removed files/directories from cache to database.");
      }
      i = 0;
      Iterator<Long> iterator = concurrentRemoveSet.iterator();
      while (iterator.hasNext()) {
        long id = iterator.next();
        if (env.equals("VOLT")) { 
          removeIds.add(id);
        } else if (env.equals("IGNITE")) {
          INode inode = INodeKeyedObjects.getCache().getIfPresent(id);
          BinaryObjectBuilder inodeKeyBuilder = conn.getIgniteClient().binary().builder("InodeKey");
          BinaryObject inodeKey = inodeKeyBuilder
            .setField("parentName", inode.getParentName())
            .setField("name", inode.getLocalName())
            .build(); 
          removeKeys.add(inodeKey);
        }
        iterator.remove();
        if (++i >= num) break;
      }

      try {
        if (removeIds.size() > 0) {
          if (env.equals("VOLT")) {
            DatabaseINode.batchRemoveINodes(removeIds);
          } else if (env.equals("IGNITE")) {
            IgniteCache<BinaryObject, BinaryObject> inodesBinary = conn.getIgniteClient().cache("inodes").withKeepBinary();
            inodesBinary.removeAll(removeKeys);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (removeSize > 0 && preRemoveSize == removeSize) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Propagate removed files/directories from cache to database.");
        }
        try {
          removeIds = new ArrayList<Long>(concurrentRemoveSet);
          Iterator<Long> iterator = concurrentRemoveSet.iterator();
          while (iterator.hasNext()) {
            long id = iterator.next();
            if (env.equals("IGNITE")) {
              INode inode = INodeKeyedObjects.getCache().getIfPresent(id);
              BinaryObjectBuilder inodeKeyBuilder = conn.getIgniteClient().binary().builder("InodeKey");
              BinaryObject inodeKey = inodeKeyBuilder
                .setField("parentName", inode.getParentName())
                .setField("name", inode.getLocalName())
                .build(); 
              removeKeys.add(inodeKey); 
            }
            iterator.remove();
          }
          if (env.equals("VOLT")) {
            DatabaseINode.batchRemoveINodes(removeIds);
          } else if (env.equals("IGNITE")) {
            IgniteCache<BinaryObject, BinaryObject> inodesBinary = conn.getIgniteClient().cache("inodes").withKeepBinary();
            inodesBinary.removeAll(removeKeys); 
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    preRemoveSize = concurrentRemoveSet.size();
    Database.getInstance().retConnection(conn);
  }

  private static void renameToDB() {
    if (concurrentRenameSet == null) {
      return;
    }
    int i = 0;
    final int num = 1024;
    long renameSize = concurrentRenameSet.size();
    if (renameSize >= num) {
      Iterator<String> iterator = concurrentRenameSet.iterator();
      if (LOG.isInfoEnabled()) {
        LOG.info("Sync rename files/directories from cache to database.");
      }

      List<Long> longAttr = new ArrayList<>();
      List<String> strAttr = new ArrayList<>();

      while (iterator.hasNext()) {
        INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
        if (inode == null) continue;
        strAttr.add(inode.getLocalName());
        if (inode.getId() == 16385) {
          strAttr.add(" ");
        } else {
          strAttr.add(inode.getParentName());
        }
        longAttr.add(inode.getParentId());
        longAttr.add(inode.getId());
        longAttr.add(inode.getModificationTime());
        longAttr.add(inode.getAccessTime());
        longAttr.add(inode.getPermissionLong());
        if (inode.isDirectory()) {
          longAttr.add(0L);
        } else {
          longAttr.add(inode.asFile().getHeaderLong());
        }
        iterator.remove();
        if (++i >= num) break;
      }
      try {
        if (strAttr.size() > 0) {
          DatabaseINode.batchRenameINodes(longAttr, strAttr);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (renameSize > 0 && preRenameSize == renameSize) {
        Iterator<String> iterator = concurrentRenameSet.iterator();
        if (LOG.isInfoEnabled()) {
          LOG.info("Propagate updated files/directories from cache to database.");
        }
        try {
          List<Long> longAttr = new ArrayList<>();
          List<String> strAttr = new ArrayList<>();
          while (iterator.hasNext()) {
            INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
            if (inode == null) continue;
            strAttr.add(inode.getLocalName());
            if (inode.getId() == 16385) {
              strAttr.add(" ");
            } else {
              strAttr.add(inode.getParentName());
            }
            longAttr.add(inode.getParentId());
            longAttr.add(inode.getId());
            longAttr.add(inode.getModificationTime());
            longAttr.add(inode.getAccessTime());
            longAttr.add(inode.getPermissionLong());
            if (inode.isDirectory()) {
              longAttr.add(0L);
            } else {
              longAttr.add(inode.asFile().getHeaderLong());
            }
            iterator.remove();
          }
          if (strAttr.size() > 0) {
            DatabaseINode.batchRenameINodes(longAttr, strAttr);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    preRenameSize = concurrentRenameSet.size();
  }

  public static void asyncUpdateDB() {
    // In HDFS, the default log buffer size is 512 * 1024 bytes, or 512 KB.
    // We assume that each object size is 512 bytes, then the size of
    // concurrentUpdateSet should be 1024 which only records INode Id.
    // Note: Using INode Id, it's easy to find INode object in cache.
    insertToDB();

    removeToDB();

    renameToDB();
  }

  public static void BackupSetToDB() {
    final Runnable updateToDB =
        new Runnable() {
          public void run() {
            asyncUpdateDB();
          }
        };

    // Creates and executes a periodic action that becomes enabled first after the given initial
    // delay (1s), and subsequently with the given delay (2s) between the termination of one
    // execution and the commencement of the next.
    long delay = 300L;
    String delayStr = System.getenv("UPDATE_DB_TIME_DELAY");
    if (delayStr != null) {
      delay = Long.parseLong(delayStr);
    }

    final ScheduledFuture<?> updateHandle =
        scheduler.scheduleWithFixedDelay(updateToDB, 100, delay, MICROSECONDS);

    scheduler.schedule(
        new Runnable() {
          public void run() {
            updateHandle.cancel(true);
          }
        },
        60 * 60 * 24,
        SECONDS);
  }

  // --------------------------------------------------------
  // caffeine cache

  public static IndexedCache<String, INode> getCache() {
    if (cache == null) {
      concurrentUpdateSet = ConcurrentHashMap.newKeySet();
      concurrentRemoveSet = ConcurrentHashMap.newKeySet();

      // async write updates to buffer
      BackupSetToDB();

      // Assuming each INode has 600 bytes, then
      // 10000000 * 600 / 2^30 = 5.58 GB.
      // The default object cache has 5.58 GB.
      int num = 10000000;
      String cacheNum = System.getenv("OBJECT_CACHE_SIZE");
      if (cacheNum != null) {
        num = Integer.parseInt(cacheNum);
      }

      // https://github.com/ben-manes/caffeine/wiki/Removal
      Caffeine<Object, Object> cfein =
          Caffeine.newBuilder()
              .removalListener(
                  (Object keys, Object value, RemovalCause cause) -> {
                    if (cause == RemovalCause.COLLECTED
                        || cause == RemovalCause.EXPIRED
                        || cause == RemovalCause.SIZE) {
                      if (LOG.isInfoEnabled()) {
                        LOG.info("Cache Evicted: INode = " + (String) keys);
                      }
                      // stored procedure: update inode in db
                      INode inode = (INode) value;
                      if (inode.isDirectory()) {
                        inode.asDirectory().updateINodeDirectory();
                      } else {
                        inode.asFile().updateINodeFile();
                        FileUnderConstructionFeature uc =
                            inode.asFile().getFileUnderConstructionFeature();
                        if (uc != null) {
                          uc.updateFileUnderConstruction(inode.getId());
                        }
                      }
                    }
                  })
              .maximumSize(num);
      cache =
          new IndexedCache.Builder<String, INode>()
              .buildFromCaffeine(cfein);
    }
    return cache;
  }

  public static Cache<String, String> getMoveCache() {
    if (move == null) {
      move =  Caffeine.newBuilder()
              .expireAfterWrite(1000, TimeUnit.MILLISECONDS)
              .build();      
    }
    return move;
  }
}
