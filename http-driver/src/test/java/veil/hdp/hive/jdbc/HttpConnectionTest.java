package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HttpConnectionTest extends BaseConnectionTest {

    @Override
    Connection createConnection() throws SQLException {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive.hdp.local:10000/default";

        return new HttpHiveDriver().connect(url, properties);
    }

}