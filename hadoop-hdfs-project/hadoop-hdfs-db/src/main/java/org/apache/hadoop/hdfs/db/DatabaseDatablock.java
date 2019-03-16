package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseDatablock {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseDatablock.class);

  private static boolean checkBlockExistence(final long blkid) {
    boolean exist = false;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql =
          "SELECT CASE WHEN EXISTS ("
              + "   SELECT * FROM datablocks WHERE blockId = ?"
              + ") "
              + "THEN CAST(1 AS BIT) "
              + "ELSE CAST(0 AS BIT) END";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blkid);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getBoolean(1) == true) {
          exist = true;
        }
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return exist;
  }

  public static void insertBlock(final long blkid, final long len, final long genStamp) {
    if (checkBlockExistence(blkid)) {
      return;
    }
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();

      String sql = "INSERT INTO datablocks(blockId, numBytes, generationStamp) VALUES (?, ?, ?);";

      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setLong(1, blkid);
      pst.setLong(2, len);
      pst.setLong(3, genStamp);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  private static <T> T getAttribute(final long id, final String attrName) {
    T result = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT " + attrName + " FROM datablocks WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ResultSetMetaData rsmd = rs.getMetaData();
        if (rsmd.getColumnType(1) == Types.BIGINT) {
          result = (T) Long.valueOf(rs.getLong(1));
        } else if (rsmd.getColumnType(1) == Types.INTEGER) {
          result = (T) Short.valueOf(rs.getString(1));
        } else if (rsmd.getColumnType(1) == Types.VARCHAR) {
          result = (T) rs.getString(1);
        }
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    LOG.info(attrName + " [GET]: (" + id + "," + result + ")");

    return result;
  }

  public static Long[] getNumBytesAndStamp(final long blockId) {
    Long[] result = new Long[2];
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT numBytes, generationStamp FROM datablocks WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        result[0] = rs.getLong(1);
        result[1] = rs.getLong(2);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    return result;
  }

  public static long getNumBytes(final long blockId) {
    return getAttribute(blockId, "numBytes");
  }

  public static long getGenerationStamp(final long blockId) {
    return getAttribute(blockId, "generationStamp");
  }

  public static short getReplication(final long blockId) {
    return getAttribute(blockId, "replication");
  }

  public static void setBlockId(final long blockId, final long bid) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE datablocks SET blockId = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, bid);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setBlockId [UPDATE]: (" + blockId + "," + bid + ")");
  }

  public static void setNumBytes(final long blockId, final long numBytes) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE datablocks SET numBytes = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, numBytes);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setNumBytes [UPDATE]: (" + blockId + "," + numBytes + ")");
  }

  public static void setGenerationStamp(final long blockId, final long generationStamp) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE datablocks SET generationStamp = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, generationStamp);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("generationStamp [UPDATE]: (" + blockId + "," + generationStamp + ")");
  }

  public static void setReplication(final long blockId, final short replication) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE datablocks SET replication = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setInt(1, replication);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setReplication [UPDATE]: (" + blockId + "," + replication + ")");
  }

  public static void delete(final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM datablocks WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void delete(final long nodeId, final int index) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "DELETE FROM datablocks WHERE blockId = ("
              + "  SELECT blockId FROM inode2block WHERE id = ? and index = ?"
              + ");";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, index);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void removeBlock(final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "BEGIN;"
              + "DELETE FROM inode2block WHERE blockId = ?;"
              + "DELETE FROM datablocks WHERE blockId = ?;"
              + "COMMIT;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void removeAllBlocks(final long inodeId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "BEGIN;"
              + "DELETE FROM datablocks WHERE blockId = ("
              + "   SELECT blockId from inode2block where id = ?"
              + ");"
              + "DELETE FROM inode2block where id = ?;"
              + "COMMIT;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, inodeId);
      pst.setLong(2, inodeId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static long getTotalNumBytes(final long inodeId, final int length) {
    long size = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "SELECT SUM(numBytes) FROM datablocks WHERE blockId IN ("
              + "  SELECT blockId FROM inode2block WHERE id = ? and index < ?"
              + ");";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, inodeId);
      pst.setInt(2, length);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        size = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getTotalNumBytes: (" + inodeId + "," + size + ")");

    return size;
  }

  public static void setECPolicyId(final long blockId, final byte ecPolicyId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE datablocks SET ecPolicyId = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setInt(1, (int) ecPolicyId);
      pst.setLong(2, blockId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setECPolicyId [UPDATE]: (" + blockId + "," + ecPolicyId + ")");
  }

  public static byte getECPolicyId(final long blockId) {
    byte ecId = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT ecPolicyId FROM datablocks WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ecId = (byte) rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    return ecId;
  }

  public static void addStorage(final long blockId, final int index, final int blockIndex) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO blockstripes(blockId, index, blockIndex) VALUES (?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setLong(1, blockId);
      pst.setInt(2, index);
      pst.setInt(3, blockIndex);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static byte getStorageBlockIndex(final long blockId, final int index) {
    byte blockIndex = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT blockIndex FROM blockstripes WHERE blockId = ? and index = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setInt(2, index);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        blockIndex = (byte) rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return blockIndex;
  }

  public static void setStorageBlockIndex(
      final long blockId, final int index, final byte blockIndex) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE blockstripes SET blockIndex = ? WHERE blockId = ? and index = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setInt(1, (int) blockIndex);
      pst.setLong(2, blockId);
      pst.setInt(3, index);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }
}
