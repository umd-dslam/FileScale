package org.apache.hadoop.hdfs.db;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.voltdb.*;
import org.voltdb.client.*;

public class Database {
  private static Database instance;
  private GenericObjectPool<DatabaseConnection> pool;
  private ExecutorService executor;

  Database() {
    try {
      initializePool();
      initializeExecutor();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
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

  public ExecutorService getExecutorService() {
    return executor;
  }

  public DatabaseConnection getConnection() {
    DatabaseConnection obj = null;
    try {
      obj = pool.borrowObject();
    } catch (Exception e) {
      System.err.println("Failed to borrow a Connection object : " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
    return obj;
  }

  public void retConnection(DatabaseConnection obj) {
    // make sure the object is returned to the pool
    if (null != obj) {
      pool.returnObject(obj);
    }
  }

  // A helper method to initialize the pool using the config and object-factory.
  private void initializePool() throws Exception {
    try {
      // We use the GenericObjectPool implementation of Object Pool as this suffices for most needs.
      // When we create the object pool, we need to pass the Object Factory class that would be
      // responsible for creating the objects.
      // Also pass the config to the pool while creation.
      pool = new GenericObjectPool<DatabaseConnection>(new DatabaseFactory());
      String num = System.getenv("MAX_CONNECTION_NUM");
      if (num == null) {
        pool.setMaxTotal(2000);
      } else {
        pool.setMaxTotal(Integer.parseInt(num));
      }

      pool.setMinIdle(16);
      pool.setMaxIdle(500);
      pool.setBlockWhenExhausted(true);
      pool.setMaxWaitMillis(30 * 1000);
      pool.preparePool();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private void initializeExecutor() throws Exception {
    try {
      String num = System.getenv("ASYNC_EXECUTOR_NUM");
      if (num == null) {
        executor = Executors.newFixedThreadPool(16);
      } else {
        executor = Executors.newFixedThreadPool(Integer.parseInt(num));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
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
