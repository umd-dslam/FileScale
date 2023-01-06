import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataBase { 
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
 
    static {
    	config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/hops");
    	config.setUsername("root");
    	config.setPassword("");
    	config.setDriverClassName("com.mysql.jdbc.Driver");
        ds = new HikariDataSource( config );
    }
 
    private DataBase() {}
 
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

  public static void main(String args[]) {
    Connection con = null;
    Statement stmt = null;
    ResultSet res = null;

    try {

      Class.forName("com.mysql.jdbc.Driver");

      con = DriverManager.getConnection("jdbc:mysql://192.168.0.10:3306/hops", "root", "");
     // con = DataBase.getConnection();

      stmt = con.createStatement();
      res = stmt.executeQuery("select * from hdfs_users;");

      while (res.next()) {

        System.out.println(res.getString(1) + "\t" + res.getString(2));
      }
    } catch (Exception e) {

      System.out.println(e);
      e.printStackTrace();
    } finally {

      try {

        con.close();
      } catch (Exception e) {

        System.out.println(e);
      }
    }
  }
}
