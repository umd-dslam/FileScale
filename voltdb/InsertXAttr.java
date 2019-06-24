import org.voltdb.*;

public class InsertXAttr extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("INSERT INTO inodexattrs(id, namespace, name, value) VALUES (?, ?, ?, ?);");

  public long run(final long id, final int namespace, final String name, final String value)
      throws VoltAbortException {
    voltQueueSQL(sql, id, namespace, name, value);
    voltExecuteSQL();
    return 1;
  }
}
