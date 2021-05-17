import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class CheckXAttrExistence extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT COUNT(id) FROM inodexattrs WHERE id = ?;");

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    return voltExecuteSQL();
  }
}
