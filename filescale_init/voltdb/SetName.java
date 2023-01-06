import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetName extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET name = ? WHERE id = ?;");

  public long run(final long id, final String name) throws VoltAbortException {
    voltQueueSQL(sql, name, id);
    voltExecuteSQL();
    return 1;
  }
}
