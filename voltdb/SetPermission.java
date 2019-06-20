import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetPermission extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET permission = ? WHERE id = ?;");

  public long run(final long id, final long permission) throws VoltAbortException {
    voltQueueSQL(sql, permission, id);
    voltExecuteSQL();
    return 1;
  }
}
