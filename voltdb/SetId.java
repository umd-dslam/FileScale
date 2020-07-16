import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetId extends VoltProcedure {

    "INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent"
    + ") SELECT "
    + " ?, name, accessTime, modificationTime, permission, header, ?"
    + " FROM inodes WHERE id = ?;");

  public final SQLStmt sql2 = new SQLStmt("DELETE FROM inodes where id = ?;");

  public long run(final long old_id, final long new_id, final long new_parent) throws VoltAbortException {
    voltQueueSQL(sql1, new_id, new_parent, old_id);
    voltExecuteSQL();
    voltQueueSQL(sql2, old_id);
    voltExecuteSQL();
    return 1;
  }
}
