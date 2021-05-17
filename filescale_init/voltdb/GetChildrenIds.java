import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetChildrenIds extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT id FROM inodes WHERE parent = ?;");

  public VoltTable[] run(long parent) throws VoltAbortException {
    voltQueueSQL(sql, parent);
    return voltExecuteSQL();
  }
}
