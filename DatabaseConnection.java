import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

public class DatabaseConnection {

    private static DatabaseConnection instance;
    private Connection connection;
    private String url = "jdbc:postgresql://localhost:5432/docker";
    private String username = "docker";
    private String password = "docker";

    private DatabaseConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            this.connection = DriverManager.getConnection(url, props);
        } catch (ClassNotFoundException ex) {
            System.out.println("Database Connection Creation Failed : " + ex.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static DatabaseConnection getInstance() throws SQLException {
        if (instance == null) {
            instance = new DatabaseConnection();
        } else if (instance.getConnection().isClosed()) {
            instance = new DatabaseConnection();
        }
        return instance;
    }

    public static void main(String [] args) {
        try {
            DatabaseConnection db = new DatabaseConnection();
            String tableName = "dir";
            Statement st = db.getConnection().createStatement();

            // Create a table
            String sqlCreate = "drop table if exists " + tableName + ";"
                + "create table if not exists " + tableName
                + "(id int primary key, parent int, name text)";
            st.execute(sqlCreate);

            // Insert into table
            st.executeUpdate("insert into " + tableName + " values "
                + "(1, NULL, 'hadoop'),"
                + "(2, 1, 'hdfs'),"
                + "(3, 2, 'src'),"
                + "(4, 2, 'test'),"
                + "(5, 3, 'fs.java'),"
                + "(6, 4, 'fs.java')");

            // Select from table
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
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
    
