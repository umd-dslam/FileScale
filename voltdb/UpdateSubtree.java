import org.voltdb.*;
import java.util.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateSubtree extends VoltProcedure {
  public final SQLStmt sql1 = new SQLStmt("SELECT id FROM inodes WHERE parent = ?;");

  public final SQLStmt sql2 = new SQLStmt(
    "SELECT id, name, accessTime, modificationTime, permission,"
    + "header, parent from inodes WHERE id = ?;");

  public final SQLStmt sql3 = new SQLStmt("INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent"
    + ") VALUES (?, ?, ?, ?, ?, ?, ?);");

  public final SQLStmt sql4 = new SQLStmt("DELETE FROM inodes where id = ?;");

  public long run(final long dir_id, final long dest_id, final long new_parent) throws VoltAbortException {
    List<Long> set = new ArrayList<>();
    set.add(dir_id);

    dest_id = 100000;

    int i = 0;
    while (i < set.size()) {
      long cid = set.get(i);
      i++;
      voltQueueSQL(sql1, cid);
      VoltTable[] res = voltExecuteSQL();
      int count = res[0].getRowCount();
      for (int j = 0; j < count; ++j) {
        set.add(res[0].fetchRow(j).getLong(0));
      }
    }
 
    for (Long child : set) {
      voltQueueSQL(sql2, child);
    }
    VoltTable[] res = voltExecuteSQL();
    Long id = null;
    String name = null;
    Long accessTime = null;
    Long modificationTime = null;
    Long permission = null;
    Long header = null;
    Long parent = null;
     for (int j = 0; j < res.length; ++j) {
      for (i = 0; i < res[j].getRowCount(); ++i) {
        VoltTableRow row = res[j].fetchRow(i);
        row.resetRowPosition();
        while (row.advanceRow()) {
         id = row.getLong(0);
         name = row.getString(1);
         accessTime = row.getLong(2);
         modificationTime = row.getLong(3);
         permission = row.getLong(4);
         header = row.getLong(5);
         parent = row.getLong(6);
        }
        if (id == dir_id) {
          id += dest_id; 
          parent = new_parent;
        } else {
          id += dest_id; 
          parent += dest_id;
        }

        voltQueueSQL(sql3,
          id,
          name,
          accessTime,
          modificationTime,
          permission,
          header,
          parent);
      }
    }
    voltExecuteSQL();    

    for (Long child : set) {
      voltQueueSQL(sql4, child);
    }
    voltExecuteSQL();
    return 1;
  }
}
