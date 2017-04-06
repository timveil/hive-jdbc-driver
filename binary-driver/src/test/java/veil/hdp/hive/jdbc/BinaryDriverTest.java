package veil.hdp.hive.jdbc;

import java.sql.Driver;
import java.sql.SQLException;

public class BinaryDriverTest extends BaseDriverTest {

    @Override
    Driver createDriver() throws SQLException {
        return new BinaryHiveDriver();
    }


}