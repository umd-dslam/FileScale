package org.apache.hadoop.hdfs.nnproxy.tools;

import org.apache.hadoop.hdfs.nnproxy.server.mount.MountsManager;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class LookupMount implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(LookupMount.class);

    Configuration conf;

    public static void main(String[] args) throws Exception {
        LoadMount main = new LookupMount();
        System.exit(ToolRunner.run(new HdfsConfiguration(), main, args));
    }

    public static String exec(String path) throw Exception {
        MountsManager mountsManager = new MountsManager();
        mountsManager.init2(new HdfsConfiguration());
        mountsManager.start();
        return mountsManager.resolve(path);
    }

    @Override
    public int run(String[] args) throws Exception {
        String path = IOUtils.toString(System.in);
        MountsManager mountsManager = new MountsManager();
        mountsManager.init(conf);
        mountsManager.start();
        String mountPoint = mountsManager.resolve(path);
        System.out.println(mountPoint);
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
