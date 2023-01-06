import org.voltdb.*;

public class RemoveAllBlocks extends VoltProcedure {

  public final SQLStmt sql1 =
      new SQLStmt(
          "DELETE FROM datablocks WHERE blockId IN (SELECT blockId from inode2block where id = ?);");
  public final SQLStmt sql2 = new SQLStmt("DELETE FROM inode2block where id = ?;");

  public long run(long id) throws VoltAbortException {
    voltQueueSQL(sql1, id);
    voltQueueSQL(sql2, id);
    voltExecuteSQL();
    return 1;
  }
}
