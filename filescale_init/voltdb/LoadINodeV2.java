import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class LoadINodeV2 extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
          "SELECT parent, parentName, id, name, permission, modificationTime, accessTime, header FROM inodes WHERE parent = ? AND name = ?;");

  public VoltTable[] run(long parentId, String childName) throws VoltAbortException {
    voltQueueSQL(sql, parentId, childName);
    return voltExecuteSQL();
  }
}
