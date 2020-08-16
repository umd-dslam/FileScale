import org.voltdb.*;

public class BatchRenameINodes extends VoltProcedure {
  public final SQLStmt sql1 =
      new SQLStmt("DELETE FROM inodes WHERE id = ?;");

  public final SQLStmt sql2 =
      new SQLStmt(
          "INSERT INTO inodes("
              + "parent, id, name, modificationTime, accessTime, permission, header, parentName"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

  public long run(
      final long[] longAttrs,
      final String[] strAttrs)
      throws VoltAbortException {
    int size = strAttrs.length / 2;
    for (int i = 0; i < size; ++i) {
      int idx = i * 6; 
      voltQueueSQL(sql1, longAttrs[idx + 1]); 
    }
    voltExecuteSQL();
    for (int i = 0; i < size; ++i) {
      int idx = i * 6;
      int idy = i * 2;
      voltQueueSQL(
          sql2,
          longAttrs[idx],
          longAttrs[idx + 1],
          strAttrs[idy],
          longAttrs[idx + 2],
          longAttrs[idx + 3],
          longAttrs[idx + 4],
          longAttrs[idx + 5],
          strAttrs[idy + 1]);
    }
    voltExecuteSQL();
    return 1;
  }
}
