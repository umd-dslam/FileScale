package org.apache.hadoop.hdfs.server.namenode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseINode2Block {
  public static void insert(final long id, final long blockId, final int index) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO inode2block(id, blockId, index) VALUES (?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setLong(1, id);
      pst.setLong(2, blockId);
      pst.setInt(3, index);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void insert(final long id, final BlockInfo[] blocks, final int index) {
    if (blocks == null || blocks.length == 0) {
      return;
    }

    String sql = "INSERT INTO inode2block(id, blockId, index) VALUES ";
    for (int i = 0; i < blocks.length; ++i) {
      sql +=
          "("
              + String.valueOf(id)
              + ","
              + String.valueOf(blocks[i].getBlockId())
              + ","
              + String.valueOf(index + i)
              + "),";
    }
    sql = sql.substring(0, sql.length() - 1) + ";";

    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      Statement st = conn.createStatement();
      st.executeUpdate(sql);
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void insert(final long id, final List<Long> blockIds, final int index) {
    if (blockIds == null || blockIds.size() == 0) {
      return;
    }

    int idx = index;
    int size = blockIds.size();
    String sql = "INSERT INTO inode2block(id, blockId, index) VALUES ";
    for (int i = 0; i < size; ++i) {
      idx += i;
      sql +=
          "("
              + String.valueOf(id)
              + ","
              + String.valueOf(blockIds[i])
              + ","
              + String.valueOf(idx)
              + "),";
    }
    sql = sql.substring(0, sql.length() - 1) + ";";

    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      Statement st = conn.createStatement();
      st.executeUpdate(sql);
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  private static <T> void setAttribute(final long id, final String attrName, final T attrValue) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();

      String sql = "UPDATE inode2block SET " + attrName + " = ? WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);

      if (attrValue instanceof String) {
        if (attrValue.toString() == null) {
          pst.setNull(1, java.sql.Types.VARCHAR);
        } else {
          pst.setString(1, attrValue.toString());
        }
      } else if (attrValue instanceof Integer || attrValue instanceof Long) {
        pst.setLong(1, ((Long) attrValue).longValue());
      } else {
        System.err.println("Only support string and long types for now.");
        System.exit(0);
      }
      pst.setLong(2, id);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info(attrName + " [UPDATE]: (" + id + "," + attrValue + ")");
  }

  private static <T> T getAttribute(final long id, final String attrName) {
    T result = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT " + attrName + " FROM inode2block WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ResultSetMetaData rsmd = rs.getMetaData();
        if (rsmd.getColumnType(1) == Types.BIGINT || rsmd.getColumnType(1) == Types.INTEGER) {
          result = (T) Long.valueOf(rs.getLong(1));
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

  public static int getNumBlocks(final long id) {
    int num = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(DISTINCT blockId) FROM inode2block WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        num = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getNumBlocks: (" + id + "," + num + ")");

    return num;
  }

  public static int getLastBlockId(final long id) {
    int blockId = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT blockId FROM inode2block WHERE id = ? ORDER BY index DESC LIMIT 1;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        blockId = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getLastBlockId: (" + id + "," + blockId + ")");

    return blockId;
  }

  public static long getBcId(final long blockId) {
    return getAttribute(blockId, "id");
  }

  public static void setBcId(final long blockId, final long bcId) {
    setAttribute(blockId, "id", bcId);
  }

  public static void setBcId(final long bcId, final long newBcId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inode2block SET id = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, newBcId);
      pst.setLong(2, bcId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static List<Long> getBlockIds(final long inodeId) {
    List<Long> blockIds = new ArrayList<>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT blockId FROM inode2block WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, inodeId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        long id = rs.getLong(1);
        blockIds.add(id);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getBlockIds: (" + blockIds + "," + inodeId + ")");

    return blockIds;
  }

  public static void delete(final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE blockId = ?;";
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
      String sql = "DELETE FROM inode2block WHERE id = ? and index = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, index);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void delete(final long nodeId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void truncate(final long nodeId, final int n) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE id = ? and index >= ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, n);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void setBlockId(final long nodeId, final int index, final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inode2block SET blockId = ? WHERE id = ? and index = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setLong(2, nodeId);
      pst.setInt(3, index);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static int getBlockId(final long nodeId, final int index) {
    int blockId = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT blockId from inode2block WHERE id = ? and index = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, index);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        blockId = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return blockId;
  }
}
