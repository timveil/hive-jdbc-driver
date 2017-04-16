package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryZookeeperConnectionTest extends BaseConnectionTest {

    @Override
    public Connection createConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large26.hdp.local:2181/default";

        return new BinaryZookeeperHiveDriver().connect(url, properties);
    }


}