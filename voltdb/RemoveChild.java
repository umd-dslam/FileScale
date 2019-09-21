import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class RemoveChild extends VoltProcedure {

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

  public VoltTable[] run(long id) throws VoltAbortException {
    voltQueueSQL(sql1, id);
    VoltTable[] results = voltExecuteSQL();

    if (results[0].getRowCount() < 1) {
      return -1;
    }
    for (int i = 0; i < results[0].getRowCount(); ++i) {
      voltQueueSQL(sql2, results[0].fetchRow(i).getLong(0));
    }
    voltExecuteSQL();
    return results;
  }
}
