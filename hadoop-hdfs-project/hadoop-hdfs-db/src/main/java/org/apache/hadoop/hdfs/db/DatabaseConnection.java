package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.lang.System;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConnection {
  private static DatabaseConnection instance;
  private Connection connection;
  private static String postgres = "jdbc:postgresql://localhost:5432/docker";
  private static String cockroach = "jdbc:postgresql://localhost:26257/docker";;
  private static String username = "docker";
  private static String password = "docker";

  static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

  private DatabaseConnection() throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");

      Properties props = new Properties();

      String env = System.getenv("DATABASE");
      if (env == null || env == "POSTGRES") {
        props.setProperty("user", username);
        props.setProperty("password", password);
        this.connection = DriverManager.getConnection(postgres, props);
      } else if (env == "COCKROACH") {
        props.setProperty("user", username);
        props.setProperty("sslmode", "disable"); 
        this.connection = DriverManager.getConnection(cockroach, props);
      }
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }

    try {
      // create inode table in Postgres
      String sql =
          "CREATE TABLE IF NOT EXISTS inodes("
              + "   id bigint primary key, parent bigint, name text,"
              + "   accessTime bigint, modificationTime bigint,"
              + "   header bigint, permission bigint"
              + ");"
              + "CREATE TABLE IF NOT EXISTS inode2block("
              + "   blockId bigint primary key, id bigint, index int"
              + ");"
              + "CREATE TABLE IF NOT EXISTS datablocks("
              + "   blockId bigint primary key, numBytes bigint, generationStamp bigint,"
              + "   replication int, ecPolicyId int"
              + ");"
              + "CREATE TABLE IF NOT EXISTS blockstripes("
              + "   blockId bigint, index int, blockIndex int,"
              + "   PRIMARY KEY(blockId, index)"
              + ");"
              + "CREATE TABLE IF NOT EXISTS block2storage("
              + "   blockId bigint, index int, storageId text,"
              + "   PRIMARY KEY(blockId, index)"
              + ");"
              + "CREATE TABLE IF NOT EXISTS storage("
              + "   storageId text primary key, storageType int, state int,"
              + "   capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,"
              + "   blockPoolUsed bigint, blockReportCount int, heartbeatedSinceFailover boolean,"
              + "   blockContentsStale boolean, datanodeUuid text"
              + ");";
      Statement st = connection.createStatement();
      st.execute(sql);

      LOG.info("DatabaseConnection: [OK] Create inodes, inode2block and datablocks in db.");

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
