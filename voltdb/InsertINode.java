import org.voltdb.*;

public class InsertINode extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
          "UPSERT INTO inodes("
              + "	id, name, accessTime, modificationTime, permission, header, parent"
              + ") VALUES (?, ?, ?, ?, ?, ?, ?);");

  public long run(
      final long id,
      final long pid,
      final String name,
      final long accessTime,
      final long modificationTime,
      final long permission,
      final long header)
      throws VoltAbortException {
    voltQueueSQL(sql, id, name, accessTime, modificationTime, permission, header, pid);
    voltExecuteSQL();
    return 1;
  }
}
