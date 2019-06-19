import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class AddChild extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPSERT INTO inodes(parent, name, id) VALUES (?, ?, ?);");

  public long run(final long childId, final String childName, final long parentId) throws VoltAbortException {
    voltQueueSQL(sql, parentId, childName, childId);
    voltExecuteSQL();
    return 1;
  }
}
