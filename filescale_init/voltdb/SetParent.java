import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetParent extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET parent = ? WHERE id = ?;");

  public long run(final long id, final long parent) throws VoltAbortException {
    voltQueueSQL(sql, parent, id);
    voltExecuteSQL();
    return 1;
  }
}
