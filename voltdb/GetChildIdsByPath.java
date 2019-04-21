import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class GetChildIdsByPath extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("SELECT id FROM inodes WHERE parent = ? and name = ?;");

  public VoltTable[] run(long id, String[] components) throws VoltAbortException {
    VoltTable[] r = new VoltTable[1];
    VoltTable t = new VoltTable(
				new VoltTable.ColumnInfo("id", VoltType.BIGINT),
				new VoltTable.ColumnInfo("name", VoltType.BIGINT));
    // add the id of root (components[0]) into t
    t.addRow(id, components[0]);

    long parent = id;
    for (int i = 1; i < components.length; ++i) {
      voltQueueSQL(sql, parent, components[i]);
      VoltTable[] results = voltExecuteSQL();
      if (results[0].getRowCount() < 1) {
        break;
      }
      parent = results[0].fetchRow(0).getLong(0);
      t.addRow(parent, components[i]);
    }

    r[0] = t;
    return r;
  }
}
