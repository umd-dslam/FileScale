import org.voltdb.*;
import java.io.File;

// https://docs.voltdb.com/tutorial/Part5.php
public class SetPermissionsV2 extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt("UPDATE inodes SET permission = ? WHERE parentName STARTS WITH ?;");
  public final SQLStmt sql2 = new SQLStmt("UPDATE inodes SET permission = ? WHERE parentName = ? and name = ?;");

  public long run(final String path, final long permission) throws VoltAbortException {
    File file = new File(path);
    String parent = file.getParent();
    String name = file.getName();
    voltQueueSQL(sql1, permission, path);
    voltQueueSQL(sql2, permission, parent, name);
    voltExecuteSQL();
    return getUniqueId();
  }
}
