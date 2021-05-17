import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class VoltDBStoredProcedureTest extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT id FROM inodes WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
