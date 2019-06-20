import org.voltdb.*;

public class DeleteViaBcId extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("DELETE FROM inode2block WHERE id = ?;");

  public long run(long nodeId) throws VoltAbortException {
    voltQueueSQL(sql, nodeId);
    voltExecuteSQL();
    return 1;
  }
}
