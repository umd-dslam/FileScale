package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseFactory extends BasePooledObjectFactory<DatabaseConnection> {

  public DatabaseFactory() {
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

  @Override
  public void activateObject(PooledObject<DatabaseConnection> pooledObject) throws Exception {
    super.activateObject(pooledObject);
  }

  @Override
  public boolean validateObject(PooledObject<DatabaseConnection> pooledObject) {
    final DatabaseConnection dbconn = pooledObject.getObject();
    try {
      if (!dbconn.getConnection().isClosed() || dbconn.getVoltClient() != null) {
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
      Connection conn = dbconn.getConnection();
      if (!conn.isClosed()) {
        try {
          conn.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      Client client = dbconn.getVoltClient();
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
