import java.util.Random;
import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetNameNode extends VoltProcedure {

  // longest prefix match
  public final SQLStmt sql1 =
      new SQLStmt(
          "SELECT namenode, path, readOnly FROM mount "
              + "WHERE ? STARTS WITH path "
              + "ORDER BY CHAR_LENGTH(path) DESC LIMIT 1;");

  // all namenodes for read only directory
  public final SQLStmt sql2 =
      new SQLStmt("SELECT namenode FROM mount WHERE readOnly = 1 AND path = ?;");

  public final Random rand = new Random();

  public VoltTable[] run(String fsdir) throws VoltAbortException {
    VoltTable[] r = new VoltTable[1];
    VoltTable t = new VoltTable(new VoltTable.ColumnInfo("namenode", VoltType.STRING));

    voltQueueSQL(sql1, fsdir);
    VoltTable[] results = voltExecuteSQL();
    if (results[0].getRowCount() < 1) {
      return results;
    }
    String namenode = results[0].fetchRow(0).getString(0);
    String path = results[0].fetchRow(0).getString(1);
    Long readOnly = results[0].fetchRow(0).getLong(2);

    if (readOnly == 1L) {
      voltQueueSQL(sql2, path);
      results = voltExecuteSQL();
      int rand_index = rand.nextInt(results[0].getRowCount());
      t.addRow(results[0].fetchRow(rand_index).getString(0));
    } else {
      t.addRow(namenode);
    }

    r[0] = t;
    return r;
  }
}
