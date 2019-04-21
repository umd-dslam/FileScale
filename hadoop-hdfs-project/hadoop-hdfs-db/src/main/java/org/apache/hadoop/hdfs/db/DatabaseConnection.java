package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.lang.System;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseConnection {
  private static DatabaseConnection instance;
  private Connection connection;
  private String postgres = "jdbc:postgresql://localhost:5432/docker";
  private String cockroach = "jdbc:postgresql://localhost:26257/docker";
  private String volt = "jdbc:voltdb://localhost:21212";
  private String username = "docker";
  private String password = "docker";
  private Client client = null;
  private ClientConfig config = null;

  static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

  private DatabaseConnection() throws SQLException {
    try {
      String url = "";
      String env = System.getenv("DATABASE");
      Properties props = new Properties();

      if (env.equals("VOLT")) {
        Class.forName("org.voltdb.jdbc.Driver");
        this.connection = DriverManager.getConnection(volt);
        this.config = new ClientConfig();
        this.config.setTopologyChangeAware(true);
        this.client = ClientFactory.createClient(config);
        this.client.createConnection("localhost", 21212);
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
      LOG.info("DatabaseConnection: [" + env + "] " + url);
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public Client getVoltClient() {
    return client;
  }


  public static void displayResults(VoltTable[] results) {
    int table = 1;
    for (VoltTable result : results) {
      System.out.printf("*** Table %d ***\n", table++);
      displayTable(result);
    }
  }

  public static void displayTable(VoltTable t) {
    final int colCount = t.getColumnCount();
    int rowCount = 1;
    t.resetRowPosition();
    while (t.advanceRow()) {
      System.out.printf("--- Row %d ---\n", rowCount++);

      for (int col = 0; col < colCount; col++) {
        System.out.printf("%s: ", t.getColumnName(col));
        switch (t.getColumnType(col)) {
          case TINYINT:
          case SMALLINT:
          case BIGINT:
          case INTEGER:
            System.out.printf("%d\n", t.getLong(col));
            break;
          case STRING:
            System.out.printf("%s\n", t.getString(col));
            break;
          case DECIMAL:
            System.out.printf("%f\n", t.getDecimalAsBigDecimal(col));
            break;
          case FLOAT:
            System.out.printf("%f\n", t.getDouble(col));
            break;
        }
      }
    }
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
