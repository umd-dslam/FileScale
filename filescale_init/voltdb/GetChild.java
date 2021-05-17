import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetChild extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT id FROM inodes WHERE parent = ? AND name = ?;");

  public VoltTable[] run(long parentId, String childName) throws VoltAbortException {
    voltQueueSQL(sql, parentId, childName);
    return voltExecuteSQL();
  }
}
