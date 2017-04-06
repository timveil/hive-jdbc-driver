package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryDriverConnectionTest extends BaseConnectionTest {


    @Override
    Connection createConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default";

        return new BinaryHiveDriver().connect(url, properties);
    }

}