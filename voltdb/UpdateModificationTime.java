import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class UpdateModificationTime extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("UPDATE inodes SET modificationTime = ("
          + "SELECT modificationTime FROM inodes WHERE id = ?) WHERE id = ?;");

  public long run(final long id, final long childId) throws VoltAbortException {
    voltQueueSQL(sql, childId, id);
    voltExecuteSQL();
    return 1;
  }
}
