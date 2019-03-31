import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.lang.System;

public class HdfsSchemaInDB {
  private static HdfsSchemaInDB instance;
  private Connection connection;
  private String postgres = "jdbc:postgresql://localhost:5432/docker";
  private String cockroach = "jdbc:postgresql://localhost:26257/docker";
	private String volt = "jdbc:voltdb://localhost:21212";
  private String username = "docker";
  private String password = "docker";

  private HdfsSchemaInDB() throws SQLException {
    try {
      String url = "";
      String env = System.getenv("DATABASE");
      Properties props = new Properties();

			if (env.equals("VOLT")) {
				Class.forName("org.voltdb.jdbc.Driver");
        this.connection = DriverManager.getConnection(volt);
				url = volt; 
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
			String sql1 = "DROP TABLE inodes IF EXISTS;"
							+ "DROP TABLE inode2block IF EXISTS;"
							+ "DROP TABLE datablocks IF EXISTS;"
							+ "DROP TABLE blockstripes IF EXISTS;"
							+ "DROP TABLE block2storage IF EXISTS;"
							+ "DROP TABLE storage IF EXISTS;";
      String sql2 =
          "CREATE TABLE inodes("
              + "   id bigint primary key, parent bigint, name varchar(30),"
              + "   accessTime bigint, modificationTime bigint,"
              + "   header bigint, permission bigint"
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
              + "   blockId bigint, idx int, storageId varchar(64),"
              + "   PRIMARY KEY(blockId, idx)"
              + ");"
              + "CREATE TABLE storage("
              + "   storageId varchar(64) primary key, storageType int, state int,"
              + "   capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,"
              + "   blockPoolUsed bigint, blockReportCount int, heartbeatedSinceFailover smallint,"
              + "   blockContentsStale smallint, datanodeUuid varchar(128)"
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

  public static HdfsSchemaInDB getInstance() throws SQLException {
    if (instance == null) {
      instance = new HdfsSchemaInDB();
    } else if (instance.getConnection().isClosed()) {
      instance = new HdfsSchemaInDB();
    }
    return instance;
  }

	public static void main(String [] args) {
		try {
			HdfsSchemaInDB.getInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
