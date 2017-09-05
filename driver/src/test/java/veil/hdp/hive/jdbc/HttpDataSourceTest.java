package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;

public class HttpDataSourceTest extends AbstractConnectionTest {
    @Override
    public Connection createConnection(String host) throws SQLException {

        HiveDataSource ds = new HiveDataSource();
        ds.setHost(host);
        ds.setPort(10501);
        ds.setTransportMode(TransportMode.http);
        ds.setDatabase("tests");

        return ds.getConnection();
    }
}
