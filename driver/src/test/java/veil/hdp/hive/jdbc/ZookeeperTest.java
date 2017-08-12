package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ZookeeperTest extends AbstractConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();

        String url = "jdbc:hive2://" + host + ":2181/jdbc_test?zkEnabled=true";

        return new HiveDriver().connect(url, properties);
    }


}