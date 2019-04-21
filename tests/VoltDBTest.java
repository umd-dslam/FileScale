import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.voltdb.*;
import org.voltdb.client.*;

public class VoltDBTest {

  private static VoltDBTest instance;
  private Connection connection;
  private String url = "jdbc:voltdb://localhost:21212";

  private Client client = null;
  private ClientConfig config = null;

  private VoltDBTest() throws SQLException {
    try {
      Class.forName("org.voltdb.jdbc.Driver");
      this.connection = DriverManager.getConnection(url);
      this.config = new ClientConfig();
      this.config.setTopologyChangeAware(true);
      this.client = ClientFactory.createClient(config);
      this.client.createConnection("localhost", 21212);
    } catch (Exception ex) {
      ex.printStackTrace();
      System.out.println("Database Connection Creation Failed : " + ex.getMessage());
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public Client getVoltClient() {
    return client;
  }

  public static VoltDBTest getInstance() throws SQLException {
    if (instance == null) {
      instance = new VoltDBTest();
    } else if (instance.getConnection().isClosed()) {
      instance = new VoltDBTest();
    }
    return instance;
  }

  public static void displayResults(VoltTable[] results) {
    int table = 1;
    for (VoltTable result : results) {
      System.out.printf("*** Table %d ***\n", table++);
      displayTable(result);
    }
  }

  public static void displayTable(VoltTable t) {
    final int colCount = t.getColumnCount();
    int rowCount = 1;
    t.resetRowPosition();
    while (t.advanceRow()) {
      System.out.printf("--- Row %d ---\n", rowCount++);

      for (int col = 0; col < colCount; col++) {
        System.out.printf("%s: ", t.getColumnName(col));
        switch (t.getColumnType(col)) {
          case TINYINT:
          case SMALLINT:
          case BIGINT:
          case INTEGER:
            System.out.printf("%d\n", t.getLong(col));
            break;
          case STRING:
            System.out.printf("%s\n", t.getString(col));
            break;
          case DECIMAL:
            System.out.printf("%f\n", t.getDecimalAsBigDecimal(col));
            break;
          case FLOAT:
            System.out.printf("%f\n", t.getDouble(col));
            break;
        }
      }
    }
  }

  public static void main(String[] args) {
    try {
      VoltDBTest db = VoltDBTest.getInstance();
      Statement st = db.getConnection().createStatement();

      // Select inodes from table
      ResultSet rs = st.executeQuery("SELECT * FROM inodes;");
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsNumber = rsmd.getColumnCount();
      while (rs.next()) {
        for (int i = 1; i <= columnsNumber; i++) {
          System.out.format("%6.6s ", rs.getString(i));
        }
        System.out.println("");
      }

      // call a stored procedure
      CallableStatement proc =
          db.getConnection().prepareCall("{call VoltDBStoredProcedureTest(?)}");
      proc.setLong(1, 1);
      rs = proc.executeQuery();
      while (rs.next()) {
        System.out.printf("%s\n", rs.getString(1));
      }

      rs.close();
      st.close();
      proc.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      // call a stored procedure
      VoltDBTest db = VoltDBTest.getInstance();
      VoltTable[] results =
          db.getVoltClient().callProcedure("VoltDBStoredProcedureTest", 2).getResults();
      displayResults(results);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
