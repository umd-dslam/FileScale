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

public class CockroachDBTest {

    private static CockroachDBTest instance;
    private Connection connection;
    private String url = "jdbc:postgresql://localhost:26257/bank";
    private String username = "docker";
    // private String password = "docker";

    private CockroachDBTest() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("sslmode", "disable");
            // props.setProperty("password", password);
            this.connection = DriverManager.getConnection(url, props);
        } catch (ClassNotFoundException ex) {
            System.out.println("Database Connection Creation Failed : " + ex.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static CockroachDBTest getInstance() throws SQLException {
        if (instance == null) {
            instance = new CockroachDBTest();
        } else if (instance.getConnection().isClosed()) {
            instance = new CockroachDBTest();
        }
        return instance;
    }

    public static void main(String [] args) {
        try {
            CockroachDBTest db = CockroachDBTest.getInstance();
            Statement st = db.getConnection().createStatement();

            // Select from table
            ResultSet rs = st.executeQuery("SELECT * FROM bank.accounts");
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
    
