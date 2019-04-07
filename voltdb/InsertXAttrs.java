import org.voltdb.*;

public class InsertXAttrs extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("INSERT INTO inodexattrs SELECT ?, namespace, name, value FROM inodexattrs WHERE id = ?;");

  public long run(long id, Long[] ids) throws VoltAbortException {
    for (int i = 0; i < ids.length; ++i) {
    	voltQueueSQL(sql, id, ids[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
