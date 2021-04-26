import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetPermissions extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET permission = ? WHERE parentName = ? and name = ?;");

  public long run(final String[] parents, final String[] names, final long permission) throws VoltAbortException {
    for (int i = 0; i < parents.length; i++) {
      voltQueueSQL(sql, permission, parents[i], names[i]);
    }
    voltExecuteSQL();
    return 1;
  }
}
