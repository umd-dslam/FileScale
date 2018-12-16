import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

public class DatabaseConnectionTest {

    private static DatabaseConnectionTest instance;
    private Connection connection;
    private String url = "jdbc:postgresql://192.168.65.3:5432/docker";
    private String username = "docker";
    private String password = "docker";

    private DatabaseConnectionTest() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            this.connection = DriverManager.getConnection(url, props);
        } catch (ClassNotFoundException ex) {
            System.out.println("Database Connection Creation Failed : " + ex.getMessage());
        }
        try {
            Connection conn = this.connection; 
            // check the existence of node in Postgres
            String sql =
            "CREATE TABLE IF NOT EXISTS inodes(id int primary key, parent int, name text);";
            Statement st = conn.createStatement();
            st.execute(sql);
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static DatabaseConnectionTest getInstance() throws SQLException {
        if (instance == null) {
            instance = new DatabaseConnectionTest();
        } else if (instance.getConnection().isClosed()) {
            instance = new DatabaseConnectionTest();
        }
        return instance;
    }

    public static boolean checkInodeExistence(final long parentId, final String childName) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql =
            "SELECT CASE WHEN EXISTS (SELECT * FROM inodes WHERE parent = ? AND name = ?)"
            + " THEN CAST(1 AS BIT)"
            + " ELSE CAST(0 AS BIT) END";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, parentId);
            pst.setString(2, childName);
            ResultSet rs = pst.executeQuery();
            while(rs.next()) {
                if (rs.getBoolean(1) == true) {
                    exist = true;
                }
            }
            rs.close();
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return exist;
    }

    public static boolean checkInodeExistence(final long childId) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql =
            "SELECT CASE WHEN EXISTS (SELECT * FROM inodes WHERE id = ?)"
            + " THEN CAST(1 AS BIT)"
            + " ELSE CAST(0 AS BIT) END";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, childId);
            ResultSet rs = pst.executeQuery();
            while(rs.next()) {
                if (rs.getBoolean(1) == true) {
                    exist = true;
                }
            }
            rs.close();
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return exist;
    }

    public static void removeChild(final long childId) {
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // delete file/directory recusively
            String sql =
                "DELETE FROM inodes WHERE id IN (" +
                "   WITH RECURSIVE cte AS (" +
                "       SELECT id, parent, name, 1 AS lev FROM inodes d WHERE id = ?" +
                "   UNION ALL" +
                "       SELECT d.id, d.parent, d.name, lev + 1 FROM cte" +
                "       JOIN dir d ON cte.id = d.parent" +
                "   )" +
                "   SELECT id FROM cte" +
                ");";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, childId);
            pst.executeUpdate();
            pst.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
        }
    }

    public static long getChild(final long parentId, final String childName) {
        long childId = -1;
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql = "SELECT id FROM inodes WHERE parent = ? AND name = ?;";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, parentId);
            pst.setString(2, childName);
            ResultSet rs = pst.executeQuery();
            while(rs.next()) {
                childId = rs.getLong(1);
            }
            rs.close();
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return childId;   
    }

    public static boolean addChild(final long childId, final String childName, final long parentId) {
        // return false if the child with this name already exists 
        if (checkInodeExistence(parentId, childName)) {
            return false;
        }

        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();

            String sql;
            if (checkInodeExistence(childId)) {
                // rename inode
                sql = "UPDATE inodes SET parent = ?, name = ? WHERE id = ?;";
            } else {
                // insert inode
                sql = "INSERT INTO inodes(parent, name, id) VALUES (?,?,?);";
            }

            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, parentId);
            pst.setString(2, childName);
            pst.setLong(3, childId);
            pst.executeUpdate();
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return true;
    }

    public static void main(String [] args) {
        try {
            DatabaseConnectionTest db = DatabaseConnectionTest.getInstance();
            String tableName = "inodes";
            Statement st = db.getConnection().createStatement();

            // Insert into table
            st.executeUpdate("insert into " + tableName + " values "
                + "(1, NULL, 'hadoop'),"
                + "(2, 1, 'hdfs'),"
                + "(3, 2, 'src'),"
                + "(4, 2, 'test'),"
                + "(5, 3, 'fs.java'),"
                + "(6, 4, 'fs.java')");

            // Select from table
            // ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            // ResultSetMetaData rsmd = rs.getMetaData();
            // int columnsNumber = rsmd.getColumnCount();
            // while (rs.next()) {
            //     for (int i = 1; i <= columnsNumber; i++) {
            //         System.out.format("%6.6s ", rs.getString(i));
            //     }
            //     System.out.println("");
            // }
            // rs.close();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        DatabaseConnectionTest.removeChild(2);
    }
}
    
