package veil.hdp.hive.jdbc;


import veil.hdp.hive.jdbc.test.BaseDriverTest;

import java.sql.Driver;
import java.sql.SQLException;


public class HttpZookeeperDriverTest extends BaseDriverTest {


    @Override
    public Driver createDriver() throws SQLException {
        return new HttpZookeeperHiveDriver();
    }


}