import org.voltdb.*;

public class InsertXAttrs extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("INSERT INTO inodexattrs(id, namespace, name, value) VALUES(?, ?, ?, ?);");

  public long run(long id, Integer[] ns, String[] namevals) throws VoltAbortException {
    for (int i = 0; i < ns.length; ++i) {
      voltQueueSQL(sql, id, ns[i], namevals[i * 2], namevals[i * 2 + 1]);
    }
    voltExecuteSQL();
    return 1;
  }
}
