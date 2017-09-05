package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractDriverTest;

import java.sql.Driver;
import java.sql.SQLException;

public class DriverTest extends AbstractDriverTest {

    @Override
    public Driver createDriver() throws SQLException {
        return new HiveDriver();
    }

}