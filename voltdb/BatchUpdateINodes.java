import org.voltdb.*;

public class BatchUpdateINodes extends VoltProcedure {

  public final SQLStmt sql1 =
      new SQLStmt(
          "UPSERT INTO inodes("
              + "parent, id, name, modificationTime, accessTime, permission, header"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?);");
  public final SQLStmt sql2 =
      new SQLStmt("UPSERT INTO inodeuc(id, clientName, clientMachine) VALUES (?, ?, ?);");

  public long run(long[] longAttrs, String[] strAttrs, long[] fileIds, String[] fileAttrs)
      throws VoltAbortException {
    for (int i = 0; i < strAttrs.length; ++i) {
      int idx = i * 6;
      voltQueueSQL(
          sql1,
          longAttrs[idx],
          longAttrs[idx + 1],
          strAttrs[i],
          longAttrs[idx + 2],
          longAttrs[idx + 3],
          longAttrs[idx + 4],
          longAttrs[idx + 5]);
    }

    for (int i = 0; i < fileIds.length; ++i) {
      int idx = i * 2;
      voltQueueSQL(sql2, fileIds[i], fileAttrs[idx], fileAttrs[idx + 1]);
    }

    voltExecuteSQL();
    return 1;
  }
}
