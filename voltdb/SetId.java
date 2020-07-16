import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetId extends VoltProcedure {

    public final SQLStmt sql1 = new SQLStmt(
      "SELECT id, name, accessTime, modificationTime, permission,"
      + "header, parent from inodes WHERE id = ?;");
  
    public final SQLStmt sql2 = new SQLStmt("DELETE FROM inodes where id = ?;");
  
    public final SQLStmt sql3 = new SQLStmt("INSERT INTO inodes("
      + "id, name, accessTime, modificationTime, permission, header, parent"
      + ") VALUES (?, ?, ?, ?, ?, ?, ?);");

  public long run(final long old_id, final long new_id, final long new_parent) throws VoltAbortException {
    voltQueueSQL(sql1, old_id);
    VoltTable[] results = voltExecuteSQL();

    voltQueueSQL(sql2, old_id);

    for (int j = 0; j < results.length; ++j) {
      for (int i = 0; i < results[j].getRowCount(); ++i) {
        voltQueueSQL(sql3,
          new_id,
          results[j].fetchRow(i).getString(1),
          results[j].fetchRow(i).getLong(2),
          results[j].fetchRow(i).getLong(3),
          results[j].fetchRow(i).getLong(4),
          results[j].fetchRow(i).getLong(5),
          new_parent);
      }
    }
    return 1;
  }
}
