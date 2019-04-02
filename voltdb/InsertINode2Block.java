import org.voltdb.*;

public class InsertINode2Block extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("INSERT INTO inode2block(id, blockId, idx) VALUES (?, ?, ?);");

  public long run(long id, Long[] bids, Long[] idxs) throws VoltAbortException {
    for (int i = 0; i < bids.length; ++i) {
    	voltQueueSQL(sql, id, bids[i], idxs[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
