import org.voltdb.*;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetId extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt("UPDATE inodes SET id = ?, parent = ?  WHERE id = ?;");

  public long run(final long old_id, final long new_id, final long new_parent) throws VoltAbortException {
    voltQueueSQL(sql, new_id, new_parent, old_id);
    voltExecuteSQL();
    return 1;
  }
}
