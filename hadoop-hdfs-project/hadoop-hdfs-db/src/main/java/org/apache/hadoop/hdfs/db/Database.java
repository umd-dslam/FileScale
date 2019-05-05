package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.voltdb.*;
import org.voltdb.client.*;

public class Database {
  private static Database instance;
  private GenericObjectPool<DatabaseConnection> pool;

  Database() {
    try {
      initializePool();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  public static void init() {
    getInstance();
  }

  public static Database getInstance() {
    if (instance == null) {
      instance = new Database();
    }
    return instance;
  }

  public Connection getConnection() {
    DatabaseConnection obj = null;
    try {
      obj = pool.borrowObject();
      try {
        // ...use the object...
        return obj.getConnection();
      } catch (Exception e) {
        // invalidate the object
        pool.invalidateObject(obj);
        // do not return the object to the pool twice
        obj = null;
      } finally {
        // make sure the object is returned to the pool
        if (null != obj) {
          pool.returnObject(obj);
        }
      }
    } catch (Exception e) {
      System.err.println("Failed to borrow a Connection object : " + e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    return null;
  }

  public Client getVoltClient() {
    DatabaseConnection obj = null;
    try {
      obj = pool.borrowObject();
      try {
        // ...use the object...
        return obj.getVoltClient();
      } catch (Exception e) {
        // invalidate the object
        pool.invalidateObject(obj);
        // do not return the object to the pool twice
        obj = null;
      } finally {
        // make sure the object is returned to the pool
        if (null != obj) {
          pool.returnObject(obj);
        }
      }
    } catch (Exception e) {
      System.err.println("Failed to borrow a Volt client object : " + e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    return null;
  }

  // A helper method to initialize the pool using the config and object-factory.
  private void initializePool() throws Exception {
    try {
      // We use the GenericObjectPool implementation of Object Pool as this suffices for most needs.
      // When we create the object pool, we need to pass the Object Factory class that would be
      // responsible for creating the objects.
      // Also pass the config to the pool while creation.
      pool = new GenericObjectPool<DatabaseConnection>(new DatabaseFactory());
      String conn = System.getenv("MAXCONNECTION");
      if (conn == null) {
        pool.setMaxTotal(2000);
      } else {
        pool.setMaxTotal(Integer.parseInt(conn));
      }

      pool.setMinIdle(50);
      pool.setMaxIdle(500);
      pool.setBlockWhenExhausted(true);
      pool.setMaxWaitMillis(30 * 1000);
      pool.preparePool();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
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
}
