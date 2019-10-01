package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseConnection {
  private static String postgres = "jdbc:postgresql://localhost:5432/docker";
  private static String cockroach = "jdbc:postgresql://localhost:26257/docker";
  private static String volt = "jdbc:voltdb://localhost:21212";
  private static String username = "docker";
  private static String password = "docker";

  private Connection connection;
  private Client client = null;

  static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

  DatabaseConnection() throws SQLException {
    try {
      String url = null;
      String host = null;
      String env = System.getenv("DATABASE");
      Properties props = new Properties();

      if (env.equals("VOLT")) {
        Class.forName("org.voltdb.jdbc.Driver");
        url = System.getenv("VOLTDB_SERVER");
        if (url == null) {
          host = "localhost";
          url = volt;
        } else {
          host = url;
          url = "jdbc:voltdb://" + url + ":21212";
        }
        this.connection = DriverManager.getConnection(url);
        ClientConfig config = new ClientConfig();
        config.setTopologyChangeAware(true);
        this.client = ClientFactory.createClient(config);
        this.client.createConnection(host, 21212);
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
      if (LOG.isInfoEnabled()) {
        LOG.info("DatabaseConnection: [" + env + "] " + url);
      }
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(-1);
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public Client getVoltClient() {
    return client;
  }
}
