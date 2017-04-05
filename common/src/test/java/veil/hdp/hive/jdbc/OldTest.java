package test.java.veil.hdp.hive.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.Properties;


public class OldTest extends BaseJunitTest {

    @Test
    public void oldDriver() throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default";

        Connection connection = DriverManager.getConnection(url, properties);

        DatabaseMetaData metaData = connection.getMetaData();

        ResultSet catalogs = metaData.getCatalogs();


        if (catalogs != null) {
            while (catalogs.next()) {
                log.debug("catalog entry {}", metaData.getCatalogs().getString("TABLE_CAT"));
            }
        }

        ResultSet schemas = metaData.getSchemas();

        if (schemas != null) {
            while (schemas.next()) {
                log.debug("schemas entry {}", schemas.getString("TABLE_SCHEM"));
            }
        }

    }
}
