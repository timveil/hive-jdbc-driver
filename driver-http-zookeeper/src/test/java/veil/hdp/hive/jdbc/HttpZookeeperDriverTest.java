package veil.hdp.hive.jdbc;


import java.sql.Driver;
import java.sql.SQLException;


public class HttpZookeeperDriverTest extends BaseDriverTest {


    @Override
    public Driver createDriver() throws SQLException {
        return new HttpZookeeperHiveDriver();
    }


}