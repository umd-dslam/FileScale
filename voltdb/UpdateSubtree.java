import org.voltdb.*;
import java.util.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateSubtree extends VoltProcedure {
  public final SQLStmt sql1 = new SQLStmt("SELECT id FROM inodes WHERE parent = ?");

  public final SQLStmt sql2 = new SQLStmt(
    "INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent"
    + ") SELECT "
    + " id + ?, name, accessTime, modificationTime, permission, header, ?"
    + " FROM inodes WHERE id = ?;");

  public final SQLStmt sql3 = new SQLStmt(
    "INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent"
    + ") SELECT "
    + " id + ?, name, accessTime, modificationTime, permission, header, parent + ?"
    + " FROM inodes WHERE id = ?;");

  public final SQLStmt sql4 = new SQLStmt("DELETE FROM inodes where id = ?;");

  // public final SQLStmt sql2 = new SQLStmt("UPDATE inodes SET id = id + ?, parent = ? WHERE id = ?;");
  // public final SQLStmt sql3 = new SQLStmt("UPDATE inodes SET id = id + ?, parent = parent + ? WHERE id = ?;");

  public long run(final long dir_id, final long dest_id, final long new_parent) throws VoltAbortException {
    List<Long> set = new ArrayList<>();
    set.add(dir_id);

    int i = 0;
    while (i < set.size()) {
      long cid = set.get(i);
      i++;
      voltQueueSQL(sql1, cid);
      VoltTable[] res = voltExecuteSQL();
      int count = res[0].getRowCount();
      if (count < 1) {
        continue;
      }
      for (int j = 0; j < count; ++j) {
        set.add(res[0].fetchRow(j).getLong(0));
      }
    }
 
    // diretory
    voltQueueSQL(sql2, dest_id, new_parent, dir_id);
 
    // childs
    for (i = 1; i < set.size(); i++) {
      voltQueueSQL(sql3, dest_id, dest_id, set.get(i));
    }
    voltExecuteSQL();

    for (Long child : set) {
      voltQueueSQL(sql4, child);
    }
    voltExecuteSQL();
    return 1;
  }
}
