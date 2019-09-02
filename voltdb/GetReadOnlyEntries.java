import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetReadOnlyEntries extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT namenode, path FROM mount WHERE readOnly = 1;");

  public VoltTable[] run() throws VoltAbortException {
    voltQueueSQL(sql);
    return voltExecuteSQL();
  }
}
