package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HttpZookeeperConnectionTest extends BaseConnectionTest {


    @Override
    Connection createConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive.hdp.local:2181/default";

        return new HttpZookeeperHiveDriver().connect(url, properties);
    }


}