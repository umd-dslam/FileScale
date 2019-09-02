import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetAccessTime extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("SELECT namenode, path, readOnly FROM mount ORDER BY namenode ASC;");

  public VoltTable[] run() throws VoltAbortException {
    voltQueueSQL(sql);
    return voltExecuteSQL();
  }
}
