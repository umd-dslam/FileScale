package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseDatablock {
  public static void insertBlock(final long blkid, final long len, final long genStamp) {
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
}