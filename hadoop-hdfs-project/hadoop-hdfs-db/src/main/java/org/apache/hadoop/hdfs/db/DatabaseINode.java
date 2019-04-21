package org.apache.hadoop.hdfs.db;

import java.sql.CallableStatement;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import java.util.HashMap;
import org.voltdb.*;

public class DatabaseINode {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseINode.class);

  public static final long LONG_NULL = 0L;

  public DatabaseINode() {}

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
      String sql = "SELECT COUNT(id) FROM inodes WHERE id = ?;";
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
    LOG.info("insertInode: (" + id + ")");
  }

  public static void setAccessTime(final long id, final long accessTime) {
    setAttribute(id, "accessTime", accessTime);
  }

  public static void setModificationTime(final long id, final long modificationTime) {
    setAttribute(id, "modificationTime", modificationTime);
  }

  public static void updateModificationTime(final long id, final long childId) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inodes SET modificationTime = ("
        + "SELECT modificationTime FROM inodes WHERE id = ?) WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      pst.setLong(2, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("updateModificationTime [UPDATE]: (" + id + ")");    
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

  public static List<Long> getChildIdsByPath(final long id, final String[] components) {
    Map<String, Long> map = new HashMap<>();
    List<Long> res = new ArrayList();
    try {
      // call a stored procedure
      VoltTable[] results = DatabaseConnection.getInstance().getVoltClient().callProcedure(
          "GetChildIdsByPath", id, components).getResults();
      VoltTable result = results[0];
      result.resetRowPosition();
      while (result.advanceRow()) {
        Long val = result.getLong(0);
        String key = result.getString(1);
        map.add(key, val);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("getChildIdsByPath: " + id);

    for (int i = 0; i < map.size(); ++i) {
      res.add(map.get(components[i]));
    }
    return res;
  }

  public static void removeChild(final long id) {
    try {
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        Connection conn = DatabaseConnection.getInstance().getConnection();
        CallableStatement proc = conn.prepareCall("{call RemoveChild(?)}");
        proc.setLong(1, id);
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
        pst.setLong(1, id);
        pst.executeUpdate();
        pst.close();
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("removeChild: " + id);
  }

  public static List<String> getPathComponents(final long childId) {
    List<String> names = new ArrayList();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent, name FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent, d.name FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT name FROM cte;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        names.add(0, rs.getString(1));
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getPathComponents: " + childId);
    return names;
  }

  // Inclusive: childId
  public static Pair<List<Long>, List<String>> getParentIdsAndPaths(final long childId) {
    List<Long> ids = new ArrayList();
    List<String> names = new ArrayList();
    ImmutablePair result = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent, name FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent, d.name FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT id, name FROM cte;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ids.add(0, rs.getLong(1));
        names.add(0, rs.getString(2));
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    LOG.info("getParentIdsAndPaths: " + childId);

    if (ids.size() != 0 || names.size() != 0) {
      result = new ImmutablePair<>(ids, names);
    }
    return result;
  }

  // Exclusive: childId
  public static List<Long> getParentIds(final long childId) {
    List<Long> parents = new ArrayList();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT id FROM cte WHERE id != ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      pst.setLong(2, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        parents.add(0, rs.getLong(1));
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getParentIds: " + childId);
    return parents;
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
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "";
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        sql = "UPSERT INTO inodes(parent, name, id) VALUES (?, ?, ?);";
      } else {
        sql = "INSERT INTO inodes(parent, name, id) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET parent = ?, name = ?;";
      }
      LOG.info("addChild: [OK] UPSERT (" + childId + "," + parentId + "," + childName + ")");

      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      pst.setString(2, childName);
      pst.setLong(3, childId);
      if (!env.equals("VOLT")) {
        pst.setLong(4, parentId);
        pst.setString(5, childName);
      }
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    return true;
  }

  public static long getINodesNum() {
    long num = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(id) FROM inodes;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        num = rs.getLong(1);
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    LOG.info("getINodesNum [GET]: (" + num + ")");

    return num;
  }

  public static long getLastInodeId() {
    long num = 0;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT MAX(id) FROM inodes;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        num = rs.getLong(1);
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    LOG.info("getLastInodeId [GET]: (" + num + ")");

    return num;
  }

  public static void insertUc(final long id, final String clientName, final String clientMachine) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO inodeuc(id, clientName, clientMachine) VALUES (?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.setString(2, clientName);
      pst.setString(3, clientMachine);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("insertUc [UPDATE]: (" + id + ", " + clientName + ", " + clientMachine + ")");
  }

  public static Boolean checkUCExistence(final long id) {
    boolean exist = false;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(id) FROM inodeuc WHERE id = ?";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
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

  public static String getUcClientName(final long id) {
    String name = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT clientName FROM inodeuc WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        name = rs.getString(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getUcClientName [GET]: (" + id + ", " + name + ")");
    return name;
  }

  public static void setUcClientName(final long id, final String clientName) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inodeuc SET clientName = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setString(1, clientName);
      pst.setLong(2, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setUcClientName [UPDATE]: (" + id + ", " + clientName + ")");
  }

  public static String getUcClientMachine(final long id) {
    String name = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT clientMachine FROM inodeuc WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        name = rs.getString(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getUcClientMachine [GET]: (" + id + ", " + name + ")"); 
    return name;
  }

  public static void setUcClientMachine(final long id, final String clientMachine) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "UPDATE inodeuc SET clientMachine = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setString(1, clientMachine);
      pst.setLong(2, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("setUcClientMachine [UPDATE]: (" + id + ", " + clientMachine + ")");
  }  

  public static void removeUc(final long id) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inodeuc WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("removeUc [UPDATE]: (" + id + ")");
  }

  public static String getXAttrValue(final long id) {
    String value = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT value FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        value = rs.getString(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getXAttrValue [GET]: (" + id + ", " + value + ")"); 
    return value;
  }

  public static String getXAttrName(final long id) {
    String name = null;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT name FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        name = rs.getString(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getXAttrName [GET]: (" + id + ", " + name + ")"); 
    return name;
  }

  public static int getXAttrNameSpace(final long id) {
    int ns = -1;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT namespace FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ns = rs.getInt(1);
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getXAttrNameSpace [GET]: (" + id + ", " + ns + ")"); 
    return ns;
  }

  public class XAttrInfo {
    public int namespace;
    public String name;
    public String value;

    public XAttrInfo(int ns, String name, String val) {
      this.namespace = ns;
      this.name = name;
      this.value = val;
    }

    public int getNameSpace() {
      return namespace;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }
  }

  public List<XAttrInfo> getXAttrs(final long id) {
    List<XAttrInfo> xinfo = new ArrayList<XAttrInfo>();
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT namespace, name, value FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        xinfo.add(new XAttrInfo(rs.getInt(1), rs.getString(2), rs.getString(3)));
      }
      rs.close();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("getXAttrs [GET]: (" + id + ")"); 
    return xinfo;
  }

  public static Boolean checkXAttrExistence(final long id) {
    boolean exist = false;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT COUNT(id) FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getInt(1) >= 1) {
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

  public static void insertXAttr(final long id, final int namespace, final String name, final String value) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "INSERT INTO inodexattrs(id, namespace, name, value) VALUES (?, ?, ?, ?);";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.setInt(2, namespace);
      pst.setString(3, name);
      pst.setString(4, value);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("insertXAttr [UPDATE]: (" + id + ", " + namespace + ", " + name + ", " + value + ")");
  }

  public static void removeXAttr(final long id) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "DELETE FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("removeXAttr [UPDATE]: (" + id + ")");
  }

  public static void insertXAttrs(final long id, final List<Integer> ns, final List<String> namevals) {
    try {
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        Connection conn = DatabaseConnection.getInstance().getConnection();
        CallableStatement proc = conn.prepareCall("{call InsertXAttrs(?, ?, ?)}");
        proc.setLong(1, id);
        proc.setArray(2, conn.createArrayOf("SMALLINT", ns.toArray(new Long[ns.size()])));
        proc.setArray(3, conn.createArrayOf("VARCHAR", namevals.toArray(new String[namevals.size()])));
        ResultSet rs = proc.executeQuery();
        while (rs.next()) {
          LOG.info("insertXAttrs Return: " + rs.getLong(1));
        }
        rs.close();
        proc.close();
      } else {
        Connection conn = DatabaseConnection.getInstance().getConnection();
        String sql = "";
        for (int i = 0; i < ns.size(); ++i) {
          sql += "INSERT INTO inodexattrs(id, namespace, name, value) VALUES(?, ?, ?, ?);";
        }
        PreparedStatement pst = conn.prepareStatement(sql);
        for (int i = 0; i < ns.size(); ++i) {
          pst.setLong(i * 4 + 1, id);
          pst.setInt(i * 4 + 2, ns.get(i));
          pst.setString(i * 4 + 3, namevals.get(i * 2));
          pst.setString(i * 4 + 4, namevals.get(i * 2 + 1));
        }
        pst.executeUpdate();
        pst.close();
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    LOG.info("insertXAttrs: " + id);
  }
}
