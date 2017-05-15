package veil.hdp.hive.jdbc;

import java.sql.Driver;
import java.sql.SQLException;

public class BinaryZookeeperDriverTest extends BaseDriverTest {

    @Override
    public Driver createDriver() throws SQLException {
        return new BinaryZookeeperHiveDriver();
    }


}