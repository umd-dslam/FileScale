package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConnection {

  public static final long LONG_NULL = 0L;
  private static DatabaseConnection instance;
  private Connection connection;
  private String url = "jdbc:postgresql://localhost:5432/docker";
  private String username = "docker";
  private String password = "docker";

  static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

  private DatabaseConnection() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");

      Properties props = new Properties();
      props.setProperty("user", username);
      props.setProperty("password", password);

      this.connection = DriverManager.getConnection(url, props);
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }

    Preconditions.checkArgument(connection != null);

    try {
      // create inode table in Postgres
      String sql =
          "DROP TABLE IF EXISTS inodes;"
              + "CREATE TABLE inodes("
              + "   id int primary key, parent int, name text,"
              + "   accessTime bigint, modificationTime bigint,"
              + "   header bigint, permission bigint"
              + ");"
              + "DROP TABLE IF EXISTS inode2block;"
              + "CREATE TABLE inode2block("
              + "   id int primary key, blockId bigint"
              + ");"
              + "DROP TABLE IF EXISTS datablocks;"
              + "CREATE TABLE datablocks("
              + "   id bigint primary key, numBytes bigint, generationStamp bigint,"
              + "   replication int, bcId bigint"
              + ");";
      Statement st = connection.createStatement();
      st.execute(sql);

      LOG.info("DatabaseConnection: [OK] Create inodes Table in Postgres.");

      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public static DatabaseConnection getInstance() throws SQLException {
    if (instance == null) {
      instance = new DatabaseConnection();
    } else if (instance.getConnection().isClosed()) {
      instance = new DatabaseConnection();
    }
    return instance;
  }
}
