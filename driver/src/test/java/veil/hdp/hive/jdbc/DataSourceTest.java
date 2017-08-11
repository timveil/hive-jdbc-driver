package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceTest extends AbstractConnectionTest {
    @Override
    public Connection createConnection(String host) throws SQLException {

        HiveDataSource ds = new HiveDataSource();
        ds.setHost(host);
        ds.setPort(10500);
        ds.setDatabase("jdbc_test");

        return ds.getConnection();
    }
}
