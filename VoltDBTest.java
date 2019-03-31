import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
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

            // Select from table
            ResultSet rs = st.executeQuery("SELECT * FROM dir");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    System.out.format("%6.6s ", rs.getString(i));
                }
                System.out.println("");
            }
            rs.close();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
    
