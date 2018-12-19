import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
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
                "CREATE TABLE inodes(" +
                "   id int primary key, parent int, name text," +
                "   accessTime bigint, modificationTime bigint," +
                "   header bigint, permission bigint" +
                ");";
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

    private static boolean checkInodeExistence(final long parentId, final String childName) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
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

    private static boolean checkInodeExistence(final long childId) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
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
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
            // delete file/directory recusively
            String sql =
                "DELETE FROM inodes WHERE id IN (" +
                "   WITH RECURSIVE cte AS (" +
                "       SELECT id, parent FROM inodes d WHERE id = ?" +
                "   UNION ALL" +
                "       SELECT d.id, d.parent FROM cte" +
                "       JOIN inodes d ON cte.id = d.parent" +
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

    private static <T> void setAttribute(final long id, final String attrName,
        final T attrValue) {
        try {
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
            
            String sql = "UPDATE inodes SET " + attrName + " = ? WHERE id = ?;";
            PreparedStatement pst = conn.prepareStatement(sql);
            
            if (attrValue instanceof String) {
                pst.setString(1, attrValue.toString()); 
            } else if (attrValue instanceof Integer || attrValue instanceof Long) {
                pst.setLong(1, ((Long)attrValue).longValue());
            } else {
                System.err.println("Only support string and long types for now.");
                System.exit(0);
            }
            pst.setLong(2, id);

            pst.executeUpdate();
            pst.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
        }
    }

    private static <T extends Comparable<T>> T getAttribute(final long id, final String attrName) {
        T result = null;
        try {
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
            String sql = "SELECT " + attrName + " FROM inodes WHERE id = ?;";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, id);
            ResultSet rs = pst.executeQuery();
            while(rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                if (rsmd.getColumnType(1) == Types.BIGINT
                ||  rsmd.getColumnType(1) == Types.INTEGER) {
                    result = (T)Long.valueOf(rs.getLong(1));
                } else if (rsmd.getColumnType(1) == Types.VARCHAR) {
                    result = (T)rs.getString(1); 
                }
            }
            rs.close();
            pst.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
        }

        return result;
    }

    public static void setAccessTime(final long id, final long accessTime) {
        setAttribute(id, "accessTime", accessTime);
    }

    public static void setModificationTime(final long id, final long modificationTime) {
        setAttribute(id, "modificationTime", modificationTime);
    }

    public static void setHeader(final long id, final long header) {
        setAttribute(id, "header", header);
    }

    public static long getAccessTime(final long id) {
        return getAttribute(id, "accessTime");
    }

    public static long getModificationTime(final long id) {
        return getAttribute(id, "modificationTime");
    }

    public static long getHeader(final long id) {
        return getAttribute(id, "header");
    }

    public static long getChild(final long parentId, final String childName) {
        long childId = -1;
        try {
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();
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
            Connection conn = DatabaseConnectionTest.getInstance().getConnection();

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
                + "(1, NULL, 'hadoop', 2019, 2020, 70),"
                + "(2, 1, 'hdfs', 2019, 2020, 70),"
                + "(3, 2, 'src', 2019, 2020, 70),"
                + "(4, 2, 'test', 2019, 2020, 70),"
                + "(5, 3, 'fs.java', 2019, 2020, 70),"
                + "(6, 4, 'fs.java', 2019, 2020, 70);");

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

        DatabaseConnectionTest.setAccessTime(2, 2000);
        System.out.println(DatabaseConnectionTest.getAccessTime(2)); 

        DatabaseConnectionTest.setModificationTime(2, 2077);
        System.out.println(DatabaseConnectionTest.getModificationTime(2));

        DatabaseConnectionTest.setHeader(2, 1121);
        System.out.println(DatabaseConnectionTest.getHeader(2));
    }
}
    
