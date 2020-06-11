import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetParents extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
    "SELECT id, name, accessTime, modificationTime, permission,"
    + "header from inodes SET parent = ?;");
  public final SQLStmt sql2 = new SQLStmt("DELETE inodes where parent = ?;");
  public final SQLStmt sql3 = new SQLStmt("INSERT INTO inodes("
    + "id, name, accessTime, modificationTime, permission, header, parent"
    + ") VALUES (?, ?, ?, ?, ?, ?, ?);");

  // VOLTDB ERROR: CONSTRAINT VIOLATION An update to a partitioning column triggered a partitioning error.
  // Updating a partitioning column is not supported. Try delete followed by insert.
  public long run(final long oldparent, final long newparent) throws VoltAbortException {
    VoltTable[] results = voltQueueSQL(sql1, oldparent);
    voltExecuteSQL();
    if (results[0].getRowCount() < 1) {
      return -1;
    }

    voltQueueSQL(sql2, oldparent);
    voltExecuteSQL();

    for (int j = 0; j < results.length; ++j) {
      for (int i = 0; i < results[j].getRowCount(); ++i) {
        voltQueueSQL(sql3,
          results[j].fetchRow(i).getLong(0),
          results[j].fetchRow(i).getString(1),
          results[j].fetchRow(i).getLong(2),
          results[j].fetchRow(i).getLong(3),
          results[j].fetchRow(i).getLong(4),
          results[j].fetchRow(i).getLong(5),
          newparent);
      }
    }
    voltExecuteSQL();
    return 1;
  }
}
