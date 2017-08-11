package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class HttpDataSourceTest extends AbstractConnectionTest {
    @Override
    public Connection createConnection(String host) throws SQLException {

        HiveDataSource ds = new HiveDataSource();
        ds.setHost(host);
        ds.setPort(10501);
        ds.setTransportMode(TransportMode.http);
        ds.setDatabase("jdbc_test");

        return ds.getConnection();
    }
}
