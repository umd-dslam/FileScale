package org.apache.hadoop.hdfs.nnproxy.server.mount;

import org.apache.hadoop.hdfs.nnproxy.ProxyConfig;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdfs.db.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Manages mount table and keep up-to-date to VoltDB.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MountsManager extends AbstractService {

    private static final Logger LOG = LoggerFactory.getLogger(MountsManager.class);

    public MountsManager() {
        super("MountsManager");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
    }

    public ImmutableList<String> getAllFs() {
        return ImmutableList.copyOf(DatabaseMountTable.getAllNameNodes());
    }

    public String resolve(String path) {;
        return DatabaseMountTable.getNameNode(path); 
    }

    /**
     * Determine whether given path is exactly a valid mount point
     *
     * @param path
     * @return
     */
    public boolean isMountPoint(String path) {
        return DatabaseMountTable.isMountPoint(path.replaceAll("/$", ""));
    }

    /**
     * Determine whether given path contains a mount point.
     * Directory is considered unified even if itself is a mount point, unless it contains another mount point.
     *
     * @param path
     * @return
     */
    public boolean isUnified(String path) {
        return DatabaseMountTable.isUnified(path.replaceAll("/$", "") + "/");
    }

    // Same to isUnified
    public boolean isMountPointOrUnified(String path) {
        boolean res = DatabaseMountTable.isUnified(path.replaceAll("/$", ""));
        if (LOG.isInfoEnabled()) {
            LOG.info("isMountPointOrUnified: (" + path + ", " + res + ")");
        }
        return res; 
    }


    @Override
    protected void serviceStart() throws Exception {}

    @Override
    protected void serviceStop() throws Exception {}

    public void dump() {
        DatabaseMountTable.dumpMountTable();
    }

    public void load(String mounts) throws Exception {
        String[] splitted = mounts.trim().split("\\s+");
        if (splitted.length < 3) {
            return;
        }

        List<String> namenodes = new ArrayList<String>();
        List<String> paths = new ArrayList<String>();
        List<Integer> readonlys = new ArrayList<Integer>();

        for (int i = 0; i < splitted.length; i = i + 3) {
            namenodes.add(splitted[i]);
            paths.add((splitted[i + 1]).replaceAll("/$", "")); 
            readonlys.add(Integer.valueOf(splitted[i + 2]));
        }

        DatabaseMountTable.insertEntries(
            namenodes.toArray(new String[namenodes.size()]),
            paths.toArray(new String[paths.size()]),
            readonlys.toArray(new Integer[readonlys.size()]));        
    }
}
