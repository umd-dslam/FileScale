import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.Array;
import java.util.Properties;

public class VoltDBTest {

    private static VoltDBTest instance;
    private Connection connection;
    private String url = "jdbc:voltdb://localhost:21212";

    private VoltDBTest() throws SQLException {
        try {
            Class.forName("org.voltdb.jdbc.Driver");
            this.connection = DriverManager.getConnection(url);
        } catch (ClassNotFoundException ex) {
            System.out.println("Database Connection Creation Failed : " + ex.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static VoltDBTest getInstance() throws SQLException {
        if (instance == null) {
            instance = new VoltDBTest();
        } else if (instance.getConnection().isClosed()) {
            instance = new VoltDBTest();
        }
        return instance;
    }

    public static void main(String [] args) {
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

            // declare the stored procedure in our schema
            st.executeUpdate("DROP PROCEDURE VoltDBStoredProcedureTest;");
            st.executeUpdate("CREATE PROCEDURE FROM CLASS VoltDBStoredProcedureTest;");
            // call a stored procedure
            CallableStatement proc = db.getConnection().prepareCall("{call VoltDBStoredProcedureTest(?)}");
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
    }
}
    
