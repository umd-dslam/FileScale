import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetDelegationKeys extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("UPSERT INTO delegationkeys(id, expiryDate, key) VALUES(?, ?, ?);");

  public long run(int[] ids, long[] dates, String[] keys) throws VoltAbortException {
    for (int i = 0; i < ids.length; ++i) {
      voltQueueSQL(sql, ids[i], dates[i], keys[i]);
    }
    results = voltExecuteSQL();
    return 1;
  }
}
