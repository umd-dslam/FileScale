package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseStorage {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseStorage.class);

  public static void insertStorage(final long blockId, final int idx, final String storageId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO block2storage(blockId, idx, storageId) VALUES (?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setInt(2, idx);
      if (storageId != null) {
        pst.setString(3, storageId);
      } else {
        pst.setNull(3, Types.VARCHAR);
      }
      pst.executeUpdate();
      pst.close();
      LOG.info("insertStorage: (" + blockId + "," + idx + "," + storageId + "): " + sql);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static int getNumStorages(final long blockId) {
    int num = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(DISTINCT storageId) FROM block2storage WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        num = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getNumStorages: (" + blockId + "," + num + ")");

    return num;
  }

  public static List<String> getStorageIds(final long blockId) {
    List<String> storageIds = new ArrayList<String>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT storageId FROM block2storage WHERE blockId = ? ORDER BY idx ASC;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        storageIds.add(rs.getString(1));
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getStorageIds: (" + blockId + "," + storageIds + "): ");

    return storageIds;
  }

  public static String getStorageId(final long blockId, final int idx) {
    String storageId = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT storageId FROM block2storage WHERE blockId = ? and idx = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setInt(2, idx);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        storageId = rs.getString(1);
      }
      rs.close();
      pst.close();
      LOG.info("getStorageId: (" + blockId + "," + idx + "," + storageId + "): " + sql);
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    return storageId;
  }

  public static void setStorage(final long blockId, final int idx, final String storageId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE block2storage SET storageId = ? WHERE blockId = ? and idx = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      if (storageId != null) {
        pst.setString(1, storageId);
      } else {
        pst.setNull(1, Types.VARCHAR);
      }
      pst.setLong(2, blockId);
      pst.setInt(3, idx);
      pst.executeUpdate();
      pst.close();
      LOG.info("setStorage: (" + storageId + "," + blockId + "," + idx + "): " + sql);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

}