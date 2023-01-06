import org.voltdb.*;

public class InsertBlock extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
        "UPSERT INTO datablocks(blockId, numBytes, generationStamp, ecPolicyId) VALUES (?, ?, ?, -1);");

  public long run(final long blkid, final long len, final long genStamp)
      throws VoltAbortException {
    voltQueueSQL(sql, blkid, len, genStamp);
    voltExecuteSQL();
    return 1;
  }
}
