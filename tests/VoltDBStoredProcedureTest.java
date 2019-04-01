import org.voltdb.*;
import java.util.List;
import java.util.ArrayList;

// https://docs.voltdb.com/tutorial/Part5.php
public class VoltDBStoredProcedureTest extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt("SELECT id FROM inodes WHERE id = ?;");
  public final SQLStmt sql2 = new SQLStmt("SELECT id FROM inodes WHERE parent = ?;");

  public Long[] run(long id) throws VoltAbortException {
    long child;
    List<Long> childs = new ArrayList<>();
    voltQueueSQL(sql1, id);
    VoltTable[] results = voltExecuteSQL();
    while (results != null && results.length > 0) {
      for (int i = 0; i < results.length; ++i) {
        for (int j = 0; j < results[i].getRowCount(); ++j) {
          child = results[i].fetchRow(j).getLong("id");
          voltQueueSQL(sql2, child);
          childs.add(child);
        }
      }
      results = voltExecuteSQL();
    }
    return childs.toArray(new Long[childs.size()]);
  }
}
