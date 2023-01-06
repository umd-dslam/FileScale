import org.voltdb.*;

public class RemoveBlock extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt("DELETE FROM inode2block WHERE blockId = ?;");
  public final SQLStmt sql2 = new SQLStmt("DELETE FROM datablocks WHERE blockId = ?;");

  public long run(long bid) throws VoltAbortException {
    voltQueueSQL(sql1, bid);
    voltQueueSQL(sql2, bid);
    voltExecuteSQL();
    return 1;
  }
}
