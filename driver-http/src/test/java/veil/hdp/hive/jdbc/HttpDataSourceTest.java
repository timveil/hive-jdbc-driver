package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.BaseConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;

public class HttpDataSourceTest extends BaseConnectionTest {
    @Override
    public Connection createConnection(String host) throws SQLException {

        HttpHiveDataSource ds = new HttpHiveDataSource();
        ds.setHost(host);
        ds.setPort(10501);
        ds.setDatabase("jdbc_test");

        return ds.getConnection();
    }
}
