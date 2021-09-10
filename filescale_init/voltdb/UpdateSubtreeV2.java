import org.voltdb.*;
import java.util.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateSubtreeV2 extends VoltProcedure {
  public final SQLStmt sql1 = new SQLStmt(
    "SELECT id, name, accessTime, modificationTime, permission,"
    + "header, parent, parentName from inodes WHERE parentName STARTS WITH ?;");

  public final SQLStmt sql2 = new SQLStmt("INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent, parentName"
    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

  public final SQLStmt sql3 = new SQLStmt("DELETE FROM inodes where parentName STARTS WITH ?;");

  public long run(final long dir_id, final long dest_id, final String old_parent_name,
    final String new_parent_name, final long new_parent) throws VoltAbortException {
    // 1. find subtree records
    voltQueueSQL(sql1, old_parent_name);
    VoltTable[] res = voltExecuteSQL();

    // 2. update subtree records
    Long id = null;
    String name = null;
    Long accessTime = null;
    Long modificationTime = null;
    Long permission = null;
    Long header = null;
    Long parent = null;
    String parentName = null;
    for (int j = 0; j < res.length; ++j) {
      for (int i = 0; i < res[j].getRowCount(); ++i) {
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
        voltQueueSQL(sql2,
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

    // 3. delete old subtree records
    voltQueueSQL(sql3, old_parent_name);
    voltExecuteSQL();

    return getUniqueId();
  }
}
