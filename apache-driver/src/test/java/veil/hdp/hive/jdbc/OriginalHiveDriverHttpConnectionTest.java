package veil.hdp.hive.jdbc;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


public class OriginalHiveDriverHttpConnectionTest extends AbstractConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10501/tests;transportMode=http;httpPath=cliservice";

        return new HiveDriver().connect(url, properties);
    }
}
