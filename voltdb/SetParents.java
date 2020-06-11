import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetParents extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET parent = ? WHERE id = ?;");

  public long run(final Long ids[], final long parent) throws VoltAbortException {
    for (int i = 0; i < ids.length; ++i) {
      voltQueueSQL(sql, parent, ids[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
