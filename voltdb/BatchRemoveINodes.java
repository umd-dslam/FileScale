import org.voltdb.*;

public class BatchRemoveINodes extends VoltProcedure {

  public final SQLStmt sql1 =
      new SQLStmt(
          "WITH RECURSIVE cte AS ("
              + "	SELECT id, parent FROM inodes d WHERE id = ?"
              + " UNION ALL"
              + " SELECT d.id, d.parent FROM cte"
              + " JOIN inodes d ON cte.id = d.parent"
              + " )"
              + " SELECT id FROM cte;");
  public final SQLStmt sql2 = new SQLStmt("DELETE FROM inodes WHERE id = ?;");

  public long run(long[] ids) throws VoltAbortException {
    for (int i = 0; i < ids.length; ++i) {
      voltQueueSQL(sql1, ids[i]);
    }
    VoltTable[] results = voltExecuteSQL();

    if (results[0].getRowCount() < 1) {
      return -1;
    }

    for (int j = 0; j < results.length; ++j) {
      for (int i = 0; i < results[j].getRowCount(); ++i) {
        voltQueueSQL(sql2, results[j].fetchRow(i).getLong(0));
      }
    }
    voltExecuteSQL();
    return 1;
  }
}
