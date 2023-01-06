import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class CheckBlockExistence extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT COUNT(blockId) FROM datablocks WHERE blockId = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
