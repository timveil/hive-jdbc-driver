package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryTest extends AbstractConnectionTest {


    @Override
    public Connection createConnection(String host) throws SQLException {


        Properties properties = new Properties();

        String url = "jdbc:hive2://" + host + ":10000/tests";

        return new HiveDriver().connect(url, properties);
    }

}