package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseFactory extends BasePooledObjectFactory<DatabaseConnection> {

  private DatabaseFactory() {
    super();
  }

  @Override
  public DatabaseConnection create() throws Exception {
    return new DatabaseConnection();
  }

  /** Use the default PooledObject implementation. */
  @Override
  public PooledObject<DatabaseConnection> wrap(DatabaseConnection dbconn) {
    return new DefaultPooledObject<DatabaseConnection>(dbconn);
  }

  @Override
  public PooledObject<DatabaseConnection> makeObject() throws Exception {
    return super.makeObject();
  }

  /** When an object is returned to the pool, clear the buffer. */
  @Override
  public void passivateObject(PooledObject<DatabaseConnection> pooledObject) {
    pooledObject.getObject().setLength(0);
  }

  @Override
  public void activateObject(PooledObject<DatabaseConnection> pooledObject) throws Exception {
    super.activateObject(pooledObject);
  }

  @Override
  public boolean validateObject(PooledObject<DatabaseConnection> pooledObject) {
    final DatabaseConnection dbconn = pooledObject.getObject();
    try {
      if (!dbconn.isClosed()) {
        return true;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void destroyObject(PooledObject<DatabaseConnection> pooledObject) {
    final DatabaseConnection dbconn = pooledObject.getObject();
    try {
      if (!dbconn.isClosed()) {
        try {
          dbconn.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}