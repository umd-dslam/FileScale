import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetBlockIds extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT blockId FROM inode2block WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
