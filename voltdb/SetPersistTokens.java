import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetPersistTokens extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
          "UPSERT INTO persisttokens(owner, renewer, realuser, issueDate,"
              + " maxDate, expiryDate, sequenceNumber, masterKeyId)"
              + " VALUES(?, ?, ?, ?, ?, ?, ?, ?);");

  public long run(
      int[] seqnumbers,
      int[] masterkeys,
      long[] issuedates,
      long[] maxdates,
      long[] expirydates,
      String[] owners,
      String[] renewers,
      String[] realusers)
      throws VoltAbortException {
    for (int i = 0; i < owners.length; ++i) {
      voltQueueSQL(
          sql,
          owners[i],
          renewers[i],
          realusers[i],
          issuedates[i],
          maxdates[i],
          expirydates[i],
          seqnumbers[i],
          masterkeys[i]);
    }
    VoltTable[] results = voltExecuteSQL();

    if (results[0].getRowCount() < 1) {
      return -1;
    }
    return 1;
  }
}
