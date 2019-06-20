import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetModificationTime extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET modificationTime = ? WHERE id = ?;");

  public long run(final long id, final long modificationTime) throws VoltAbortException {
    voltQueueSQL(sql, modificationTime, id);
    voltExecuteSQL();
    return 1;
  }
}
