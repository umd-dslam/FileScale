import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetStringTable extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("UPSERT INTO stringtable(id, str) VALUES(?, ?);");

  public long run(int[] ids, String[] strs) throws VoltAbortException {
    for (int i = 0; i < ids.length; ++i) {
        voltQueueSQL(sql, ids[i], strs[i]);
    }
    VoltTable[] results = voltExecuteSQL();

    if (results[0].getRowCount() < 1) {
      return -1;
    }
    return 1;
  }
}
