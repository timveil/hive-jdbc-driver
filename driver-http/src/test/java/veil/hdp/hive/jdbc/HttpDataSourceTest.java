package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.BaseConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HttpDataSourceTest extends BaseConnectionTest {
    @Override
    public Connection createConnection(String host) throws SQLException {


        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10501/jdbc_test";


        HttpHiveDataSource ds = new HttpHiveDataSource();
        ds.setProperties(properties);
        ds.setUrl(url);



        return ds.getConnection();
    }
}
