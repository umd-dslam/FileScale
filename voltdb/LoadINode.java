import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class LoadINode extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
          "SELECT parent, id, name, permission, modificationTime, accessTime, header FROM inodes WHERE id = ?;");

  public VoltTable[] run(long parentId, String childName) throws VoltAbortException {
    voltQueueSQL(sql, parentId, childName);
    return voltExecuteSQL();
  }
}
