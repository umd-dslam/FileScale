import org.voltdb.*;

public class BatchUpdateINodes extends VoltProcedure {

  public final SQLStmt sql1 =
      new SQLStmt(
          "UPSERT INTO inodes("
              + "parent, id, name, modificationTime, accessTime, permission, header, parentName"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
  public final SQLStmt sql2 =
      new SQLStmt("UPSERT INTO inodeuc(id, clientName, clientMachine) VALUES (?, ?, ?);");

  public long run(
      final long[] longAttrs,
      final String[] strAttrs,
      final long[] fileIds,
      final String[] fileAttrs)
      throws VoltAbortException {
    int size = strAttrs.length / 2;
    for (int i = 0; i < size; ++i) {
      int idx = i * 6;
      int idy = i * 2;
      voltQueueSQL(
          sql1,
          longAttrs[idx],
          longAttrs[idx + 1],
          strAttrs[idy],
          longAttrs[idx + 2],
          longAttrs[idx + 3],
          longAttrs[idx + 4],
          longAttrs[idx + 5],
          strAttrs[idy + 1]);
    }

    for (int i = 0; i < fileIds.length; ++i) {
      int idx = i * 2;
      voltQueueSQL(sql2, fileIds[i], fileAttrs[idx], fileAttrs[idx + 1]);
    }

    voltExecuteSQL();
    return getTxnId();
  }
}
