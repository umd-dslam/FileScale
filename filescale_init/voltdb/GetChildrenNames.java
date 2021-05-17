import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetChildrenNames extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT name FROM inodes WHERE parent = ?;");

  public VoltTable[] run(long parent) throws VoltAbortException {
    voltQueueSQL(sql, parent);
    return voltExecuteSQL();
  }
}
