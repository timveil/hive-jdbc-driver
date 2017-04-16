package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryDriverConnectionTest extends BaseConnectionTest {


    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10000/default";

        return new BinaryHiveDriver().connect(url, properties);
    }

}