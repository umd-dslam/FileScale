package org.apache.hadoop.hdfs.db;

import dnl.utils.text.table.TextTable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseMountTable {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseMountTable.class);

  public DatabaseMountTable() {}

  public static void insertEntries(
      final String[] namenodes, final String[] paths, final Integer[] readonlys) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure("InsertMountEntries", namenodes, paths, readonlys);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql =
            "INSERT INTO mount("
                + " namenode, path, readOnly"
                + ") VALUES (?, ?, ?) ON CONFLICT(namenode, path) DO NOTHING;";
        sql = StringUtils.repeat(sql, namenodes.length);

        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);
        for (int i = 0; i < namenodes.length; ++i) {
          pst.setString(i * 3 + 1, namenodes[i]);
          pst.setString(i * 3 + 2, paths[i]);
          pst.setLong(i * 3 + 3, readonlys[i]);
        }
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("insertEntries ...");
    }
  }

  public static List<String> getAllNameNodes() {
    List<String> res = new ArrayList();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetAllNameNodes").getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res.add(result.getString(0));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql = "SELECT namenode FROM namenodes;";

        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);

        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res.add(rs.getString(1));
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getAllNameNodes ...");
    }
    return res;
  }

  public static String getNameNode(String filePath) {
    String res = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetNameNode", filePath).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getString(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql =
            "SELECT namenode, path, readOnly FROM mount "
                + "WHERE ? STARTS WITH path "
                + "ORDER BY CHAR_LENGTH(path) DESC LIMIT 1;";
        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setString(1, filePath);
        ResultSet rs = pst.executeQuery();
        String namenode = null;
        String path = null;
        Long readOnly = null;
        while (rs.next()) {
          namenode = rs.getString(1);
          path = rs.getString(2);
          readOnly = rs.getLong(3);
        }

        if (namenode != null) {
          if (readOnly == 1L) {
            sql =
                "SELECT namenode FROM mount WHERE readOnly = 1 AND path = ? ORDER BY random() LIMIT 1;";
            pst = conn.prepareStatement(sql);
            pst.setString(1, path);
            rs = pst.executeQuery();
            while (rs.next()) {
              res = rs.getString(1);
            }
          } else {
            res = namenode;
          }
        }

        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getNameNode: (" + filePath + ", " + res + ")");
    }
    return res;
  }

  public static Boolean isMountPoint(String filePath) {
    Boolean res = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("IsMountPoint", filePath).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            if (result.getLong(0) != 0L) {
              res = true;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql = "SELECT COUNT(*) FROM mount WHERE path = ?;";
        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setString(1, filePath);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          if (rs.getLong(1) != 0L) {
            res = true;
          }
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("isMountPoint: (" + filePath + ", " + res + ")");
    }
    return res;
  }

  public static Boolean isUnified(String filePath) {
    Boolean res = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("IsUnified", filePath).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            if (result.getLong(0) != 0L) {
              res = true;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql = "SELECT COUNT(*) FROM mount WHERE path LIKE ?%;";
        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setString(1, filePath);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          if (rs.getLong(1) != 0L) {
            res = true;
          }
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("isUnified: (" + filePath + ", " + res + ")");
    }
    return res;
  }

  public static void dumpMountTable() {
    System.out.print("============================================");
    System.out.print("               Mount Table                  ");
    System.out.print("============================================");
    try {
      DatabaseConnection obj = new DatabaseConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("DumpMountTable").getResults();
          VoltTable result = results[0];
          Object[][] tuples = new Object[result.getRowCount()][];
          String[] columnNames = {"NameNode", "Path", "ReadOnly"};

          int i = 0;
          result.resetRowPosition();
          while (result.advanceRow()) {
            tuples[i++] =
                new Object[] {result.getString(0), result.getString(1), result.getLong(2)};
          }

          TextTable tt = new TextTable(columnNames, tuples);
          // this adds the numbering on the left
          tt.setAddRowNumbering(true);
          // sort by the first column
          tt.setSort(0);
          tt.printTable();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        StringBuilder res = new StringBuilder();
        String sql = "SELECT namenode, path, readOnly FROM mount ORDER BY namenode ASC;";
        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res.append(rs.getString(1));
          res.append('\t');
          res.append(rs.getString(2));
          res.append('\t');
          res.append(rs.getLong(3));
          res.append('\n');
        }
        rs.close();
        pst.close();

        if (res.length() != 0) {
          System.out.print(res.toString());
        }
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }
}
