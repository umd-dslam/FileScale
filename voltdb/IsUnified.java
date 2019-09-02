import org.voltdb.*;

public class IsUnified extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT COUNT(*) FROM mount WHERE path STARTS WITH ?;");

  public long run(String path) throws VoltAbortException {
    voltQueueSQL(sql, path);
    return voltExecuteSQL();
  }
}
