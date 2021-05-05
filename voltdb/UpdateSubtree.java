import org.voltdb.*;
import java.util.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateSubtree extends VoltProcedure {
  public final SQLStmt sql1 = new SQLStmt("SELECT id FROM inodes WHERE parent = ?;");

  public final SQLStmt sql2 = new SQLStmt(
    "SELECT id, name, accessTime, modificationTime, permission,"
    + "header, parent, parentName from inodes WHERE id = ?;");

  public final SQLStmt sql3 = new SQLStmt("INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent, parentName"
    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

  public final SQLStmt sql4 = new SQLStmt("DELETE FROM inodes where id = ?;");

  public long run(final long dir_id, final long dest_id, final String old_parent_name,
    final String new_parent_name, final long new_parent) throws VoltAbortException {
    List<Long> set = new ArrayList<>();
    set.add(dir_id);

    int i = 0;
    while (i < set.size()) {
      while (i < set.size()) {
        voltQueueSQL(sql1, set.get(i));
        i++;
      }
      VoltTable[] res = voltExecuteSQL();
      for (int j = 0; j < res.length; ++j) {
        for (int k = 0; k < res[j].getRowCount(); ++k) {
          VoltTableRow row = res[j].fetchRow(k);
	        set.add(row.getLong(0));
        }
      }
    }

    for (Long child : set) {
      voltQueueSQL(sql2, child);
    }
    VoltTable[] res = voltExecuteSQL();

    for (Long child : set) {
      voltQueueSQL(sql4, child);
    }
    voltExecuteSQL();

    Long id = null;
    String name = null;
    Long accessTime = null;
    Long modificationTime = null;
    Long permission = null;
    Long header = null;
    Long parent = null;
    String parentName = null;
    for (int j = 0; j < res.length; ++j) {
      for (i = 0; i < res[j].getRowCount(); ++i) {
        VoltTableRow row = res[j].fetchRow(i);
        id = row.getLong(0);
        name = row.getString(1);
        accessTime = row.getLong(2);
        modificationTime = row.getLong(3);
        permission = row.getLong(4);
        header = row.getLong(5);
        parent = row.getLong(6);
        parentName = row.getString(7);

        if (id == dir_id) {
          id += dest_id;
          parent = new_parent;
          parentName = new_parent_name;
        } else {
          id += dest_id;
          parent += dest_id;
          parentName = new_parent_name + parentName.substring(old_parent_name.length());
        }
        voltQueueSQL(sql3,
          id,
          name,
          accessTime,
          modificationTime,
          permission,
          header,
          parent,
          parentName);
      }
    }
    voltExecuteSQL();

    return getTxnId();
  }
}
