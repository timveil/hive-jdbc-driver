package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.BaseConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryZookeeperConnectionTest extends BaseConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");
        properties.setProperty("zkNamespace", "hiveserver2-hive2");

        String url = "jdbc:hive2://" + host + ":2181/?";

        return new BinaryZookeeperHiveDriver().connect(url, properties);
    }


}