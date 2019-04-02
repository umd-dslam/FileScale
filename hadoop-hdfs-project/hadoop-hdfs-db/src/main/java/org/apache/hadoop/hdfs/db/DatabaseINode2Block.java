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

public class DatabaseINode2Block {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseINode2Block.class);

  public static void insert(final long id, final long blockId, final int idx) {
    LOG.info("INode2Block [insert]: (" + id + "," + blockId + "," + idx + ")");
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO inode2block(id, blockId, idx) VALUES (?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setLong(1, id);
      pst.setLong(2, blockId);
      pst.setInt(3, idx);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void insert(final long id, final List<Long> blockIds, final int index) {
    if (blockIds == null || blockIds.size() == 0) {
      return;
    }

    String env = System.getenv("DATABASE");
    if (env.equals("VOLT")) {
      // call a stored procedure
      Connection conn = DatabaseConnection.getInstance().getConnection();
      CallableStatement proc = conn.prepareCall("{call InsertINode2Block(?, ?, ?)}");

      proc.setLong(1, inodeId);
      proc.setArray(2, conn.createArrayOf("BIGINT", blockIds.toArray(new Long[blockIds.size()])));
      List<Integer> idxs = new ArrayList<Integer>();
      for (int i = 0; i < blockIds.size(); ++i) {
        idxs.add(index + i);
      } 
      proc.setArray(3, conn.createArrayOf("INT", idxs.toArray(new Integer[blockIds.size()])));

      rs = proc.executeQuery();
      while (rs.next()) {
        LOG.info("INode2Block Insertion Return: " + rs.getLong(1));
      }
      rs.close();
      proc.close();
    } else {
      int idx = index;
      int size = blockIds.size();
      String sql = "INSERT INTO inode2block(id, blockId, idx) VALUES ";
      for (int i = 0; i < size; ++i) {
        idx += 1;
        sql +=
            "("
                + String.valueOf(id)
                + ","
                + String.valueOf(blockIds.get(i))
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
      LOG.info("INode2Block [insert]: (" + sql + ")");
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
      String sql = "SELECT blockId FROM inode2block WHERE id = ? ORDER BY idx DESC LIMIT 1;";
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
    long id = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT id FROM inode2block WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        id = rs.getLong(1);
      }
      rs.close();
      pst.close();
      LOG.info("getBcId: (" + blockId + "," + id + ")");
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    return id;
  }

  public static long getSize() {
    long size = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(blockId) FROM inode2block;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        size = rs.getLong(1);
      }
      rs.close();
      st.close();
      LOG.info("getSize: (" + size + ")");
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    return size;    
  }

  public static void setBcIdViaBlkId(final long blockId, final long bcId) {
    setAttribute(blockId, "id", bcId);
  }

  public static void setBcIdViaBcId(final long bcId, final long newBcId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inode2block SET id = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, newBcId);
      pst.setLong(2, bcId);
      pst.executeUpdate();
      pst.close();
      LOG.info("setBcIdViaBcId: (" + bcId + "," + newBcId + "," + sql + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static List<Long> getBlockIds(final long inodeId) {
    List<Long> blockIds = new ArrayList<>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
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
    return blockIds;
  }

  public static List<Long> getAllBlockIds() {
    List<Long> blockIds = new ArrayList<>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT blockId FROM inode2block;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        long id = rs.getLong(1);
        blockIds.add(id);
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }
    return blockIds;
  }

  public static void deleteViaBlkId(final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE blockId = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.executeUpdate();
      pst.close();
      LOG.info("deleteViaBlkId: (" + blockId + "," + sql + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void delete(final long nodeId, final int idx) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE id = ? and idx = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, idx);
      pst.executeUpdate();
      pst.close();
      LOG.info("delete: (" + nodeId + "," + idx + "," + sql + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void deleteViaBcId(final long nodeId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.executeUpdate();
      pst.close();
      LOG.info("deleteViaBcId: (" + nodeId + "," + sql + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void truncate(final long nodeId, final int n) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inode2block WHERE id = ? and idx >= ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, n);
      pst.executeUpdate();
      pst.close();
      LOG.info("truncate: (" + nodeId + "," + n + "," + sql + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void setBlockId(final long nodeId, final int idx, final long blockId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inode2block SET blockId = ? WHERE id = ? and idx = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, blockId);
      pst.setLong(2, nodeId);
      pst.setInt(3, idx);
      pst.executeUpdate();
      pst.close();
      LOG.info("setBlockId: (" + nodeId + "," + blockId + "," + idx + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static int getBlockId(final long nodeId, final int idx) {
    int blockId = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT blockId from inode2block WHERE id = ? and idx = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, nodeId);
      pst.setInt(2, idx);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        blockId = rs.getInt(1);
      }
      rs.close();
      pst.close();
      LOG.info("getBlockId: (" + nodeId + "," + blockId + ")");
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return blockId;
  }
}
