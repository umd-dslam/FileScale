import java.util.ArrayList;
import java.util.List;
import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class RemoveChild extends VoltProcedure {

  // CTE only support single partition query
  // public final SQLStmt sql1 =
  //     new SQLStmt(
  //         "WITH RECURSIVE cte AS ("
  //             + "	SELECT id, parent FROM inodes d WHERE id = ?"
  //             + " UNION ALL"
  //             + " SELECT d.id, d.parent FROM cte"
  //             + " JOIN inodes d ON cte.id = d.parent"
  //             + " )"
  //             + " SELECT id FROM cte;");

  // public final SQLStmt sql2 = new SQLStmt("DELETE FROM inodes WHERE id = ?;");

  // public long run(long id) throws VoltAbortException {
  //   voltQueueSQL(sql1, id);
  //   VoltTable[] results = voltExecuteSQL();

  //   if (results[0].getRowCount() < 1) {
  //     return -1;
  //   }
  //   for (int i = 0; i < results[0].getRowCount(); ++i) {
  //     voltQueueSQL(sql2, results[0].fetchRow(i).getLong(0));
  //   }
  //   voltExecuteSQL();
  //   return 1;
  // }
  public final SQLStmt sql0 = new SQLStmt("SELECT header FROM inodes WHERE id = ?;");
  public final SQLStmt sql1 = new SQLStmt("SELECT id FROM inodes WHERE parent = ?");
  public final SQLStmt sql2 = new SQLStmt("DELETE FROM inodes WHERE id = ?;");

  public long run(long id) throws VoltAbortException {
    voltQueueSQL(sql0, id);
    VoltTable[] results = voltExecuteSQL();
    long header = 0;
    for (int j = 0; j < results[0].getRowCount(); ++j) {
      header = results[0].fetchRow(j).getLong(0);
    }

    if (header != 0) {
      voltQueueSQL(sql2, id);
    } else {
      List<Long> set = new ArrayList<>();
      set.add(id);
      voltQueueSQL(sql2, id);

      int i = 0;
      while (i < set.size()) {
        long cid = set.get(i);
        i++;
        voltQueueSQL(sql1, cid);
        results = voltExecuteSQL();
        if (results[0].getRowCount() < 1) {
          continue;
        }
        for (int j = 0; j < results[0].getRowCount(); ++j) {
          set.add(results[0].fetchRow(j).getLong(0));
        }
      }

      for (Long kid : set) {
        voltQueueSQL(sql2, kid);
      }
    }

    voltExecuteSQL();
    return 1;
  }
}
