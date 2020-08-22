import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class LoadINodeV3 extends VoltProcedure {

  public final SQLStmt sql =
      new SQLStmt(
          "SELECT parent, parentName, id, name, permission, modificationTime, accessTime, header FROM inodes WHERE parentName = ? AND name = ?;");

  public VoltTable[] run(long parentName, String childName) throws VoltAbortException {
    voltQueueSQL(sql, parentName, childName);
    return voltExecuteSQL();
  }
}
