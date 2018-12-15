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
            System.out.println(ex.getMessage());
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

    public static boolean addChild(final long childId, final String childName, final long parentId) {
        if (getChild(parentId, childName) != -1) {
            LOG.info("addChild: [EXIST] (" + parentId + "," + childName + ")");
            return false;
        }
        try {
            Connection conn = DatabaseConnection.getInstance().getConnection();
            // add node into Postgres
            String sql = "INSERT INTO inodes(id, name, parent) VALUES (?,?,?);";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setLong(1, childId);
            pst.setString(2, childName);
            pst.setLong(3, parentId);
            pst.executeUpdate();
            LOG.info("addChild: [OK] INSERT (" + childId + "," + parentId + "," + childName + ")");
            pst.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return true;
    }
}