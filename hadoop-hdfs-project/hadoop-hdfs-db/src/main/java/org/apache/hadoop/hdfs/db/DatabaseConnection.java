package org.apache.hadoop.hdfs.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Collection;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

public class DatabaseConnection {
  private static String postgres = "jdbc:postgresql://localhost:5432/docker";
  private static String cockroach = "jdbc:postgresql://localhost:26257/docker";
  private static String volt = "jdbc:voltdb://localhost:21212";
  private static String ignite = "jdbc:ignite:thin://localhost:10800";
  private static String username = "docker";
  private static String password = "docker";

  private Connection connection;
  private Client volt_client = null;
  private IgniteEx ignite_client = null;

  static final Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

  DatabaseConnection() throws SQLException {
    try {
      String url = null;
      String host = null;
      String env = System.getenv("DATABASE");
      Properties props = new Properties();

      if (env.equals("VOLT")) {
        Class.forName("org.voltdb.jdbc.Driver");
        url = System.getenv("VOLTDB_SERVER");
        if (url == null) {
          host = "localhost";
          url = volt;
        } else {
          host = url;
          url = "jdbc:voltdb://" + url + ":21212";
        }
        this.connection = DriverManager.getConnection(url);
        ClientConfig config = new ClientConfig();
        config.setTopologyChangeAware(true);
        this.volt_client = ClientFactory.createClient(config);
        this.volt_client.createConnection(host, 21212);
      } else if (env.equals("IGNITE")) {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        url = System.getenv("IGNITE_SERVER");
        String ip = null;
        if (url == null) {
          ip = "localhost";
          url = ignite;
        } else {
          ip = url;
          url = "jdbc:ignite:thin://" + url + ":10800"; 
        }
        this.connection = DriverManager.getConnection(url);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList(ip + ":47500..47507"));
        discoverySpi.setIpFinder(ipFinder);
    
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(discoverySpi).setPeerClassLoadingEnabled(true);
        //data storage configuration
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);

        Ignition.setClientMode(true);
        this.ignite_client = (IgniteEx)Ignition.start(cfg);
      } else if (env.equals("COCKROACH")) {
        Class.forName("org.postgresql.Driver");
        props.setProperty("user", username);
        props.setProperty("sslmode", "disable");
        this.connection = DriverManager.getConnection(cockroach, props);
        url = cockroach;
      } else {
        Class.forName("org.postgresql.Driver");
        props.setProperty("user", username);
        props.setProperty("password", password);
        this.connection = DriverManager.getConnection(postgres, props);
        url = postgres;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("DatabaseConnection: [" + env + "] " + url);
      }
    } catch (Exception ex) {
      System.err.println("Database Connection Creation Failed : " + ex.getMessage());
      ex.printStackTrace();
      System.exit(-1);
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public Client getVoltClient() {
    return volt_client;
  }

  public IgniteEx getIgniteClient() {
    return ignite_client;
  }
}
