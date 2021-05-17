import java.util.*;
import org.voltdb.*;

public class RemoveINodeNoRecursive extends VoltProcedure {
  public final SQLStmt sql = new SQLStmt("DELETE FROM inodes WHERE id = ?;");

  public long run(long id) throws VoltAbortException {
    voltQueueSQL(sql, id);
    voltExecuteSQL();
    return 1;
  } 
}
