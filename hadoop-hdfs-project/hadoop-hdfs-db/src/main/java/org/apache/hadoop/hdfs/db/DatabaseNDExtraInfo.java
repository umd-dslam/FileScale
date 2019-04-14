package org.apache.hadoop.hdfs.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseNDExtraInfo {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseNDExtraInfo.class);

  void setStringTableSummary(int numEntry, int maskBits) {
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "";
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        sql = "UPSERT INTO hdfs(id, numEntry, maskBits) VALUES(0, ?, ?);";
      } else {
        sql = "INSERT INTO hdfs(id, numEntry, maskBits) VALUES(0, ?, ?) ON CONFLICT DO UPDATE;";
      }
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setInt(1, numEntry);
      pst.setInt(2, maskBits);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  Pair<Integer, Integer> getStringTableSummary() {
    Pair<Integer, Integer> result;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT numEntry, maskBits FROM hdfs;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        result = new Pair<Integer, Integer>(rs.getInt(1), rs.getInt(2));
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return result;
  }

  List<Pair<Integer, String>> getStringTable(int size) {
    List<Pair<Integer, String>> result = new ArrayList<>(size);
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT id, str FROM stringtable;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        result.add(new Pair<>(rs.getInt(1), rs.getString(2)));
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return result;
  }

  void setStringTable(int[] ids, String[] strs) {
    if (ids == null || ids.length == 0 || strs == null || strs.length == 0) {
      return;
    }

    try {
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        Connection conn = DatabaseConnection.getInstance().getConnection();
        CallableStatement proc = conn.prepareCall("{call SetStringTable(?, ?)}");

        proc.setArray(1, conn.createArrayOf("INT", ids));
        proc.setArray(2, conn.createArrayOf("VARCHAR", strs));

        ResultSet rs = proc.executeQuery();
        while (rs.next()) {
          LOG.info("setStringTable Insertion Return: " + rs.getLong(1));
        }
        rs.close();
        proc.close();
      } else {
        // INSERT INTO hdfs(id, numEntry, maskBits) VALUES(0, ?, ?) ON CONFLICT DO UPDATE;"
        String sql = "INSERT INTO stringtable(id, str) VALUES ";
        for (int i = 0; i < ids.length; ++i) {
          sql += "(" + String.valueOf(ids[i]) + "," + strs[i] + "),";
        }
        sql = sql.substring(0, sql.length() - 1) + " ON CONFLICT DO UPDATE;";

        Connection conn = DatabaseConnection.getInstance().getConnection();
        Statement st = conn.createStatement();
        st.executeUpdate(sql);
        st.close();
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
  }

  Pair<Integer, Integer> getSecretManagerSummary() {
    Pair<Integer, Integer> result;
    try {
      Connection conn = DatabaseConnection.getInstance().getConnection();
      String sql = "SELECT currentId, tokenSequenceNumber FROM hdfs;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        result = new Pair<Integer, Integer>(rs.getInt(1), rs.getInt(2));
      }
      rs.close();
      st.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return result;
  }
}
