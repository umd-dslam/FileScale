import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Collection;
import java.util.Collections;

import org.apache.ignite.*;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

public class HdfsMetaInfoSchema {
  private static HdfsMetaInfoSchema instance;
  private Connection connection;
  private String postgres = "jdbc:postgresql://localhost:5432/docker";
  private String cockroach = "jdbc:postgresql://localhost:26257/docker";
  private String volt = "jdbc:voltdb://localhost:21212";
  private String ignite = "jdbc:ignite:thin://localhost:10800";
  private String username = "docker";
  private String password = "docker";
  private IgniteEx ignite_client = null;

  private HdfsMetaInfoSchema() throws SQLException {
    String env = System.getenv("DATABASE");
    try {
      String url = null;
      Properties props = new Properties();

      if (env.equals("VOLT")) {
        Class.forName("org.voltdb.jdbc.Driver");
        url = System.getenv("VOLTDB_SERVER");
        if (url == null) {
          url = volt;
        } else {
          url = "jdbc:voltdb://" + url + ":21212"; 
        }
        this.connection = DriverManager.getConnection(url);
      } else if (env.equals("IGNITE")) {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("localhost:47500..49112"));
        discoverySpi.setIpFinder(ipFinder);
    
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(discoverySpi).setPeerClassLoadingEnabled(true);
        //data storage configuration
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);

        Ignition.setClientMode(true);
        ignite_client = (IgniteEx)Ignition.start(cfg);

        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        url = System.getenv("IGNITE_SERVER");
        if (url == null) {
          url = ignite;
        } else {
          url = "jdbc:ignite:thin://" + url + ":10800"; 
        }
        this.connection = DriverManager.getConnection(url);
      } else if (env.equals("COCKROACH")) {
        Class.forName("org.postgresql.Driver");
        props.setProperty("user", username);
        props.setProperty("sslmode", "disable");
        this.connection = DriverManager.getConnection(cockroach, props);
        url = cockroach;
      } else {
        Class.forName("org.postgresql.Driver");
        props.setProperty("user", username);
        props.setProperty("password", password);
        this.connection = DriverManager.getConnection(postgres, props);
        url = postgres;
      }
      System.out.println("HdfsSchemaInDB: [" + env + "] " + url);
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }

    try {
      // create inode table in Postgres
      String sql1 = "";
      String[] tableNames = new String[] {
        "hdfs", "namespace", "inodes", "namenodes", "mount", "stringtable",
        "inodexattrs", "inodeuc", "inode2block", "datablocks", "blockstripes",
        "block2storage", "storage", "delegationkeys", "persisttokens"};
      for (String tableName : tableNames) {
        if (env.equals("VOLT")) {
          sql1 += String.format("DROP TABLE %s IF EXISTS;", tableName);
        } else {
          sql1 += String.format("DROP TABLE IF EXISTS %s;", tableName);
        }
      }

      String sql2 =
      "CREATE TABLE hdfs(" +
      "   id int primary key, numEntry int, maskBits int," +
      "   currentId int, tokenSequenceNumber int, numKeys int, numTokens int" +
      ")";
      if (env.equals("IGNITE")) {
        sql2 += " with \"template=replicated,backups=1, cache_name=hdfs, key_type=HDFSKey, value_type=HDFS\";";
      }

      String sql3 =
      "CREATE TABLE namespace("
      + "   namespaceId int primary key, genstampV1 bigint, genstampV2 bigint,"
      + "   genstampV1Limit bigint, lastAllocatedBlockId bigint,"
      + "   transactionId bigint, rollingUpgradeStartTime bigint,"
      + "   lastAllocatedStripedBlockId bigint"
      + ")";
      if (env.equals("IGNITE")) {
        sql3 += " with \"template=replicated,backups=1, cache_name=namespace, key_type=NamespaceKey, value_type=Namespace\";";
      }

      String sql4 =
      "CREATE TABLE mount("
      + "   namenode varchar, path varchar, readOnly int,"
      + "   PRIMARY KEY(namenode, path)"
      + ")";
      if (env.equals("IGNITE")) {
        sql4 += " with \"template=replicated,backups=1, cache_name=mount, key_type=MountKey, value_type=Mount\";";
      }

      String sql5 =
      "CREATE TABLE stringtable("
      + "   id int primary key, str varchar"
      + ")";
      if (env.equals("IGNITE")) {
        sql5 += " with \"template=replicated,backups=1, cache_name=stringtable, key_type=StringTableKey, value_type=StringTable\";";
      }

      String sql6 =
      "CREATE TABLE delegationkeys("
      + "   id int primary key, expiryDate bigint, key varchar"
      + ")";
      if (env.equals("IGNITE")) {
        sql6 += " with \"template=replicated,backups=1, cache_name=delegationkeys, key_type=Delegationkey, value_type=DelegationKeys\";";
      }
    
      String sql7 =
      "CREATE TABLE persisttokens("
      + "   version int, owner varchar, renewer varchar, realuser varchar, issueDate bigint,"
      + "   maxDate bigint, sequenceNumber int primary key, masterKeyId int, expiryDate bigint"
      + ")";
      if (env.equals("IGNITE")) {
        sql7 += " with \"template=replicated,backups=1, cache_name=persisttokens, key_type=PersistTokensKey, value_type=PersistTokens\";";
      }

      String sql8 =
      "CREATE TABLE inodes("
      + "   id bigint, parent bigint NOT NULL, parentName varchar NOT NULL, name varchar,"
      + "   accessTime bigint, modificationTime bigint,"
      + "   header bigint, permission bigint,"
      + "   PRIMARY KEY (parentName, name)"
      + ")";
      if (env.equals("IGNITE")) {
        sql8 += " with \"template=partitioned,backups=1,affinityKey=parentName,cache_name=inodes,key_type=InodeKey,value_type=Inode\";";
        sql8 += "CREATE INDEX inode_idx ON inodes (id) inline_size 9;";
      } else if (env.equals("VOLT")) {
        sql8 += "; PARTITION TABLE inodes ON COLUMN parentName;";
        sql8 += "CREATE ASSUMEUNIQUE INDEX inode_id ON inodes(id);";
      }

      String sql9 =
      "CREATE TABLE inodexattrs("
      + "   id bigint primary key, namespace smallint, name varchar, value varchar"
      + ")";
      if (env.equals("IGNITE")) {
        sql9 += " with \"template=replicated,backups=1, cache_name=inodexattrs, key_type=InodeXattrsKey, value_type=InodeXattrs\";";
      }

      String sql10 =
      "CREATE TABLE inodeuc("
      + "   id bigint primary key, clientName varchar, clientMachine varchar"
      + ")";
      if (env.equals("IGNITE")) {
        sql10 += " with \"template=replicated,backups=1, cache_name=inodeuc, key_type=InodeUcKey, value_type=InodeUc\";";
      }

      String sql11 =
      "CREATE TABLE inode2block("
      + "   blockId bigint primary key, id bigint, idx int"
      + ")";
      if (env.equals("IGNITE")) {
        sql11 += " with \"template=replicated,backups=1, cache_name=inode2block, key_type=Inode2blockKey, value_type=Inode2block\";";
      }

      String sql12 =
      "CREATE TABLE datablocks("
      + "   blockId bigint primary key, numBytes bigint, generationStamp bigint,"
      + "   replication int, ecPolicyId int"
      + ")";
      if (env.equals("IGNITE")) {
        sql12 += " with \"template=replicated,backups=1, cache_name=datablocks, key_type=DatablocksKey, value_type=Datablocks\";";
      }

      String sql13 =
      "CREATE TABLE blockstripes("
      + "   blockId bigint, idx int, blockIndex int,"
      + "   PRIMARY KEY(blockId, idx)"
      + ")";
      if (env.equals("IGNITE")) {
        sql13 += " with \"template=replicated,backups=1, cache_name=blockstripes, key_type=BlockstripesKey, value_type=Blockstripes\";";
      }

      String sql14 =
      "CREATE TABLE block2storage("
      + "   blockId bigint, idx int, storageId varchar,"
      + "   PRIMARY KEY(blockId, idx)"
      + ")";
      if (env.equals("IGNITE")) {
        sql14 += " with \"template=replicated,backups=1, cache_name=block2storage, key_type=Block2storageKey, value_type=Block2storage\";";
      }

      String sql15 =
      "CREATE TABLE storage("
      + "   storageId varchar primary key, storageType int, state int,"
      + "   capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,"
      + "   blockPoolUsed bigint, blockReportCount int, heartbeatedSinceFailover smallint,"
      + "   blockContentsStale smallint, datanodeUuid varchar"
      + ")";
      if (env.equals("IGNITE")) {
        sql15 += " with \"template=replicated,backups=1, cache_name=storage, key_type=StorageKey, value_type=Storage\";";
      }

      // + "CREATE VIEW namenodes("
      // + "   namenode"
      // + ") AS SELECT DISTINCT namenode FROM mount;"

      Statement st = connection.createStatement();
      st.execute(sql1);
      st.execute(sql2);
      st.execute(sql3);
      st.execute(sql4);
      st.execute(sql5);
      st.execute(sql6);
      st.execute(sql7);
      st.execute(sql8);
      st.execute(sql9);
      st.execute(sql10);
      st.execute(sql11);
      st.execute(sql12);
      st.execute(sql13);
      st.execute(sql14);
      st.execute(sql15);
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    // key-value API test
		IgniteCluster cluster = ignite_client.cluster();
    cluster.active(true);
    cluster.enableWal("inodes");
		cluster.baselineAutoAdjustEnabled(false);
 
    Collection<String> collection = ignite_client.cacheNames();
    System.out.println("cache names = " + collection);

    IgniteCache<BinaryObject, BinaryObject> inodesBinary = ignite_client.cache("inodes").withKeepBinary();
    System.out.println(">> Updating inode record:");

    BinaryObjectBuilder inodeKeyBuilder = ignite_client.binary().builder("InodeKey");
    BinaryObject inodeKey = inodeKeyBuilder.setField("parentName", "/").setField("name", "hello").build();
    BinaryObjectBuilder inodeBuilder = ignite_client.binary().builder("INode");
    BinaryObject inode = inodeBuilder
      .setField("id", 11111L, Long.class)
      .setField("parent", 0L, Long.class)
      .setField("parentName", "/")
      .setField("name", "hello")
      .setField("accessTime", 22222L, Long.class)
      .setField("modificationTime", 33333L, Long.class)
      .setField("header", 0L, Long.class)
      .setField("permission", 777L, Long.class)
      .build();
    System.out.printf("The dir: %s, id: %s \n", inode.field("parentName"), inode.field("name"), inode.field("id"));
    inodesBinary.put(inodeKey, inode);

    IgniteCompute compute = ignite_client.compute();
    // Execute closure on all cluster nodes.
    IgniteCallable<String> call = new WalPointerTask(); 
    String res = compute.call(call);
    System.out.printf("Last Wal pointer: " + res);
    ignite_client.close();

    // SQL test
    try {
      Statement st = connection.createStatement();
      st.execute("delete from inodes where id = 11111;");
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    } 
  }

  public Connection getConnection() {
    return connection;
  }

  public static HdfsMetaInfoSchema getInstance() throws SQLException {
    if (instance == null) {
      instance = new HdfsMetaInfoSchema();
    } else if (instance.getConnection().isClosed()) {
      instance = new HdfsMetaInfoSchema();
    }
    return instance;
  }

  public static void main(String[] args) {
    try {
      HdfsMetaInfoSchema.getInstance();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
