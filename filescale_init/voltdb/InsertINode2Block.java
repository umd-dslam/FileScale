import org.voltdb.*;

public class InsertINode2Block extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("INSERT INTO inode2block(blockId, id, idx) VALUES(?, ?, ?);");

  public long run(long id, long[] bids, int[] idxs) throws VoltAbortException {
    for (int i = 0; i < bids.length; ++i) {
      voltQueueSQL(sql, bids[i], id, idxs[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
