package veil.hdp.hive.jdbc;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


public class OriginalHiveDriverConnectionTest extends BaseConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10500/jdbc_test";

        return new HiveDriver().connect(url, properties);
    }
}
