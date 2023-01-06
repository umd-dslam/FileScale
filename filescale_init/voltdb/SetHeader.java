import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetHeader extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET header = ? WHERE id = ?;");

  public long run(final long id, final long header) throws VoltAbortException {
    voltQueueSQL(sql, header, id);
    voltExecuteSQL();
    return 1;
  }
}
