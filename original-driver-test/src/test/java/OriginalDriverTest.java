import org.apache.hive.jdbc.HiveDriver;
import veil.hdp.hive.jdbc.BaseDriverTest;

import java.sql.Driver;
import java.sql.SQLException;

public class OriginalDriverTest extends BaseDriverTest {

    @Override
    public Driver createDriver() throws SQLException {
        return new HiveDriver();
    }
}