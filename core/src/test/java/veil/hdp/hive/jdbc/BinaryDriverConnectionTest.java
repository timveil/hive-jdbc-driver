package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryDriverConnectionTest extends BaseConnectionTest {


    @Override
    public Connection createConnection(String host) throws SQLException {


        Properties properties = new Properties();

        // user is not needed when authmode set to NONE
        //properties.setProperty("user", "xxx");

        String url = "jdbc:hive2://" + host + ":10500/jdbc_test";

        return new HiveDriver().connect(url, properties);
    }

}