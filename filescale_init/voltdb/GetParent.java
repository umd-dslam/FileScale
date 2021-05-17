import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetParent extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT parent FROM inodes WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
