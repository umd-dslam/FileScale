import org.voltdb.*;

public class IsMountPoint extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT COUNT(*) FROM mount WHERE path = ?;");

  public long run(String path) throws VoltAbortException {
    voltQueueSQL(sql, path);
    return voltExecuteSQL();
  }
}
