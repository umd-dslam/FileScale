import org.voltdb.*;

public class InsertMountEntries extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("UPSERT INTO mount(namenode, path, readOnly) VALUES (?, ?, ?);");

  public long run(final String[] namenodes, final String[] paths, final Integer[] readonlys)
      throws VoltAbortException {
    for (int i = 0; i < namenodes.length; ++i) {
      voltQueueSQL(sql, namenodes[i], paths[i], readonlys[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
