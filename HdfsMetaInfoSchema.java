import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class HdfsMetaInfoSchema {
  private static HdfsMetaInfoSchema instance;
  private Connection connection;
  private String postgres = "jdbc:postgresql://localhost:5432/docker";
  private String cockroach = "jdbc:postgresql://localhost:26257/docker";
  private String volt = "jdbc:voltdb://localhost:21212";
  private String username = "docker";
  private String password = "docker";

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
          "CREATE TABLE hdfs("
              + "   id int primary key, numEntry int, maskBits int,"
              + "   currentId int, tokenSequenceNumber int, numKeys int, numTokens int"
              + ");"
              + "CREATE TABLE namespace("
              + "   namespaceId int, genstampV1 bigint, genstampV2 bigint,"
              + "   genstampV1Limit bigint, lastAllocatedBlockId bigint,"
              + "   transactionId bigint, rollingUpgradeStartTime bigint,"
              + "   lastAllocatedStripedBlockId bigint"
              + ");"
              + "CREATE TABLE mount("
              + "   namenode varchar, path varchar, readOnly int,"
              + "   PRIMARY KEY(namenode, path)"
              + ");"
              + "CREATE VIEW namenodes("
              + "   namenode"
              + ") AS SELECT DISTINCT namenode FROM mount;"
              + "CREATE TABLE stringtable("
              + "   id int primary key, str varchar"
              + ");"
              + "CREATE TABLE delegationkeys("
              + "   id int primary key, expiryDate bigint, key varchar"
              + ");"
              + "CREATE TABLE persisttokens("
              + "   version int, owner varchar, renewer varchar, realuser varchar, issueDate bigint,"
              + "   maxDate bigint, sequenceNumber int primary key, masterKeyId int, expiryDate bigint"
              + ");"
              + "CREATE TABLE inodes("
              + "   id bigint primary key, parent bigint NOT NULL, name varchar,"
              + "   accessTime bigint, modificationTime bigint,"
              + "   header bigint, permission bigint"
              + ");"
              + "PARTITION TABLE inodes ON COLUMN parent;"
              + "CREATE TABLE inodexattrs("
              + "   id bigint, namespace smallint, name varchar, value varchar"
              + ");"
              + "CREATE TABLE inodeuc("
              + "   id bigint primary key, clientName varchar, clientMachine varchar"
              + ");"
              + "CREATE TABLE inode2block("
              + "   blockId bigint primary key, id bigint, idx int"
              + ");"
              + "CREATE TABLE datablocks("
              + "   blockId bigint primary key, numBytes bigint, generationStamp bigint,"
              + "   replication int, ecPolicyId int"
              + ");"
              + "CREATE TABLE blockstripes("
              + "   blockId bigint, idx int, blockIndex int,"
              + "   PRIMARY KEY(blockId, idx)"
              + ");"
              + "CREATE TABLE block2storage("
              + "   blockId bigint, idx int, storageId varchar,"
              + "   PRIMARY KEY(blockId, idx)"
              + ");"
              + "CREATE TABLE storage("
              + "   storageId varchar primary key, storageType int, state int,"
              + "   capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,"
              + "   blockPoolUsed bigint, blockReportCount int, heartbeatedSinceFailover smallint,"
              + "   blockContentsStale smallint, datanodeUuid varchar"
              + ");";
      Statement st = connection.createStatement();
      st.execute(sql1);
      st.execute(sql2);
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
