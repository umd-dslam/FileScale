package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseINode {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseINode.class);

  public static final long LONG_NULL = 0L;

  public static boolean checkInodeExistence(final long parentId, final String childName) {
    boolean exist = false;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT COUNT(id) FROM inodes WHERE parent = ? and name = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      pst.setString(2, childName);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getInt(1) == 1) {
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

  private static boolean checkInodeExistence(final long childId) {
    boolean exist = false;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT COUNT(id) FROM inodes WHERE id = ?";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getInt(1) == 1) {
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

  private static <T> void setAttribute(final long id, final String attrName, final T attrValue) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();

      String sql = "UPDATE inodes SET " + attrName + " = ? WHERE id = ?;";
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
      String sql = "SELECT " + attrName + " FROM inodes WHERE id = ?;";
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

  public static void insertInode(
      final long id,
      final long pid,
      final String name,
      final long accessTime,
      final long modificationTime,
      final long permission,
      final long header) {
    if (checkInodeExistence(id)) {
      return;
    }
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();

      String sql =
          "INSERT INTO inodes("
              + "	id, name, accessTime, modificationTime, permission, header, parent"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?);";

      PreparedStatement pst = conn.prepareStatement(sql);

      pst.setLong(1, id);
      if (name == null) {
        pst.setNull(2, java.sql.Types.VARCHAR);
      } else {
        pst.setString(2, name);
      }
      pst.setLong(3, accessTime);
      pst.setLong(4, modificationTime);
      pst.setLong(5, permission);
      pst.setLong(6, header);
      pst.setLong(7, pid);

      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  public static void setAccessTime(final long id, final long accessTime) {
    setAttribute(id, "accessTime", accessTime);
  }

  public static void setModificationTime(final long id, final long modificationTime) {
    setAttribute(id, "modificationTime", modificationTime);
  }

  public static void setPermission(final long id, final long permission) {
    setAttribute(id, "permission", permission);
  }

  public static void setHeader(final long id, final long header) {
    setAttribute(id, "header", header);
  }

  public static void setParent(final long id, final long parent) {
    setAttribute(id, "parent", parent);
  }

  public static void setName(final long id, final String name) {
    setAttribute(id, "name", name);
  }

  public static long getAccessTime(final long id) {
    return getAttribute(id, "accessTime");
  }

  public static long getModificationTime(final long id) {
    return getAttribute(id, "modificationTime");
  }

  public static long getHeader(final long id) {
    return getAttribute(id, "header");
  }

  public static long getPermission(final long id) {
    return getAttribute(id, "permission");
  }

  public static long getParent(final long id) {
    return getAttribute(id, "parent");
  }

  public static String getName(final long id) {
    return getAttribute(id, "name");
  }

  public static long getChild(final long parentId, final String childName) {
    long childId = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT id FROM inodes WHERE parent = ? AND name = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      pst.setString(2, childName);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        childId = rs.getLong(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getChild: (" + childId + "," + parentId + "," + childName + ")");

    return childId;
  }

  public static void removeChild(final long childId) {
    try {
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        Connection conn = DatabaseConnection.getInstance().getConnection();
        CallableStatement proc = conn.prepareCall("{call RemoveChild(?)}");
        proc.setLong(1, childId);
        ResultSet rs = proc.executeQuery();
        while (rs.next()) {
            LOG.info("removeChild Return: " + rs.getLong(1));
        }
        rs.close();
        proc.close();
      } else {
        Connection conn = DatabaseConnection.getInstance().getConnection();
        // delete file/directory recusively
        String sql =
            "DELETE FROM inodes WHERE id IN ("
                + "   WITH RECURSIVE cte AS ("
                + "       SELECT id, parent FROM inodes d WHERE id = ?"
                + "   UNION ALL"
                + "       SELECT d.id, d.parent FROM cte"
                + "       JOIN inodes d ON cte.id = d.parent"
                + "   )"
                + "   SELECT id FROM cte"
                + ");";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, childId);
        pst.executeUpdate();
        pst.close();
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("removeChild: " + childId);
  }

  public static List<Long> getChildrenIds(final long parentId) {
    List<Long> childIds = new ArrayList<>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT id FROM inodes WHERE parent = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        long id = rs.getLong(1);
        childIds.add(id);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    LOG.info("getChildrenIds: (" + childIds + "," + parentId + ")");

    return childIds;
  }

  public static boolean addChild(final long childId, final String childName, final long parentId) {
    // return false if the child with this name already exists
    if (checkInodeExistence(parentId, childName)) {
      LOG.info("addChild: [EXIST] (" + parentId + "," + childName + ")");
      return false;
    }

    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();

      String sql;
      if (checkInodeExistence(childId)) {
        // rename inode
        sql = "UPDATE inodes SET parent = ?, name = ? WHERE id = ?;";
        LOG.info("addChild: [OK] UPDATE (" + childId + "," + parentId + "," + childName + ")");
      } else {
        // insert inode
        sql = "INSERT INTO inodes(parent, name, id) VALUES (?,?,?);";
        LOG.info("addChild: [OK] INSERT (" + childId + "," + parentId + "," + childName + ")");
      }

      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      pst.setString(2, childName);
      pst.setLong(3, childId);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    return true;
  }
}
