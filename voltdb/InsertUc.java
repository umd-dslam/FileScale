import org.voltdb.*;

public class InsertUc extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt("INSERT INTO inodeuc(id, clientName, clientMachine) VALUES (?, ?, ?);");

  public long run(final long id, final String clientName, final String clientMachine)
      throws VoltAbortException {
    voltQueueSQL(sql, id, clientName, clientMachine);
    voltExecuteSQL();
    return 1;
  }
}
