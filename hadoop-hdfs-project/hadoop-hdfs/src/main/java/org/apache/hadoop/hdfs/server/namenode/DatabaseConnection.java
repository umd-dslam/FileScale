package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConnection {
    
    private static DatabaseConnection instance;
    private Connection connection;
    private String url = "jdbc:postgresql://192.168.65.3:5432/docker";
    private String username = "docker";
    private String password = "docker";

    static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

    private DatabaseConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");

            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);

            this.connection = DriverManager.getConnection(url, props);
        } catch (Exception ex) {
            System.err.println("Database Connection Creation Failed : " + ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }

        Preconditions.checkArgument(connection != null);

        try {
            // create inode table in Postgres
            String sql =
                "DROP TABLE IF EXISTS inodes;" +
                "CREATE TABLE inodes(id int primary key, parent int, name text);";
            Statement st = connection.createStatement();
            st.execute(sql);

            LOG.info("DatabaseConnection: [OK] Create inodes Table in Postgres.");

            st.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
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

    public static boolean checkInodeExistence(final long parentId, final String childName) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql =
                "SELECT CASE WHEN EXISTS (" +
                "   SELECT * FROM inodes WHERE parent = ? AND name = ?" +
                ") " +
                "THEN CAST(1 AS BIT) " +
                "ELSE CAST(0 AS BIT) END";
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
            System.err.println(ex.getMessage());
        }
        return exist;
    }

    public static boolean checkInodeExistence(final long childId) {
        boolean exist = false;
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql =
                "SELECT CASE WHEN EXISTS (" +
                "   SELECT * FROM inodes WHERE id = ?" +
                ") " +
                "THEN CAST(1 AS BIT) " +
                "ELSE CAST(0 AS BIT) END";
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
            System.err.println(ex.getMessage());
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

        LOG.info("getChild: (" + childId + "," + parentId + "," + childName + ")");

        return childId;   
    }

    public static List<Long> getChildrenList(final long parentId){

        List<Long> childIds = new ArrayList<>(INodeDirectory.DEFAULT_FILES_PER_DIRECTORY);
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // check the existence of node in Postgres
            String sql = "SELECT id FROM inodes WHERE parent = ?;";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, parentId);
            ResultSet rs = pst.executeQuery();
            while(rs.next()) {
                long id = rs.getLong(1);
                childIds.add(id);
            }
            rs.close();
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        LOG.info("getChild: (" + childIds + "," + parentId + ")");

        return childIds;
    }

    public static boolean addChild(final long childId, final String childName, final long parentId) {
        // return false if the child with this name already exists 
        if (checkInodeExistence(parentId, childName)) {
            LOG.info("addChild: [EXIST] (" + parentId + "," + childName + ")");
            return false;
        }

        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();

            String sql;
            if (checkInodeExistence(childId)) {
                // rename inode
                sql = "UPDATE inodes SET parent = ?, name = ? WHERE id = ?;";
                LOG.info("addChild: [OK] UPDATE (" + childId + "," + parentId + "," + childName + ")");
            } else {
                // insert inode
                sql = "INSERT INTO inodes(parent, name, id) VALUES (?,?,?);";
                LOG.info("addChild: [OK] INSERT (" + childId + "," + parentId + "," + childName + ")");
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

}