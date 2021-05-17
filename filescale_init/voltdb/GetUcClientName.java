import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetUcClientName extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT clientName FROM inodeuc WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
