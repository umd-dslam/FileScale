import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetModificationTime extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT modificationTime FROM inodes WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
