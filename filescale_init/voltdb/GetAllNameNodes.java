import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetAllNameNodes extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT namenode FROM namenodes;");

  public VoltTable[] run() throws VoltAbortException {
    voltQueueSQL(sql);
    return voltExecuteSQL();
  }
}
