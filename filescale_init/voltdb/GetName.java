import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetName extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT name FROM inodes WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
