import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetAccessTime extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET accessTime = ? WHERE id = ?;");

  public long run(final long id, final long accessTime) throws VoltAbortException {
    voltQueueSQL(sql, accessTime, id);
    voltExecuteSQL();
    return 1;
  }
}
