import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateModificationTime extends VoltProcedure {
  public final SQLStmt sql1 = new SQLStmt("SELECT modificationTime FROM inodes WHERE id = ?");
  public final SQLStmt sql2 = new SQLStmt("UPDATE inodes SET modificationTime = ? WHERE id = ?;");

  public long run(final long id, final long childId) throws VoltAbortException {
    voltQueueSQL(sql1, childId);
    VoltTable[] results = voltExecuteSQL();
    if (results[0].getRowCount() < 1) {
      return -1;
    }

    Long mtime = results[0].fetchRow(0).getLong(0);
    voltQueueSQL(sql2, mtime, id);
    voltExecuteSQL();
    return 1;
  }
}
