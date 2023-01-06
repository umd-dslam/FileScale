import org.voltdb.*;

public class RenameINode extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt("DELETE FROM inodes WHERE id = ?;");
  public final SQLStmt sql2 =
      new SQLStmt(
          "UPSERT INTO inodes("
              + "	id, name, accessTime, modificationTime, permission, header, parent, parentName"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

  public long run(
      final long id,
      final long pid,
      final String name,
      final long accessTime,
      final long modificationTime,
      final long permission,
      final long header,
      final String parentName)
      throws VoltAbortException {
    voltQueueSQL(sql1, id);
    voltExecuteSQL();
    voltQueueSQL(sql2, id, name, accessTime, modificationTime, permission, header, pid, parentName);
    voltExecuteSQL();
    return 1;
  }
}
