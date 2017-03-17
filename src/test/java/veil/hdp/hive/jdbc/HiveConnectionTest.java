package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

public class HiveConnectionTest extends BaseJunitTest {

    Connection connection = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default?transport.mode=binary";

        connection = new HiveDriver().connect(url, properties);

    }

    @After
    public void tearDown() throws Exception {

        if (connection != null) {
            connection.close();
        }
    }


    @Test
    public void getMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: {}", metaData.getDriverVersion());
        log.debug("database product version: {}", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: {}", metaData.supportsTransactions());

        ResultSet catalogs = metaData.getCatalogs();

        metaData.supportsAlterTableWithAddColumn();

        if (catalogs != null) {
            while (catalogs.next()) {
                log.debug("TABLE_CAT {}", catalogs.getString("TABLE_CAT"));
            }
        }

        ResultSet schemas = metaData.getSchemas();

        if (schemas != null) {
            while (schemas.next()) {
                log.debug("TABLE_SCHEM {}", schemas.getString("TABLE_SCHEM"));
                log.debug("TABLE_CATALOG {}", schemas.getString("TABLE_CATALOG"));
            }
        }

        ResultSet typeInfo = metaData.getTypeInfo();

        if (typeInfo != null) {
            while (typeInfo.next()) {
                log.debug("TYPE_NAME {}", typeInfo.getString("TYPE_NAME"));
            }
        }

        ResultSet tables = metaData.getTables(null, null, null, null);

        if (tables != null) {
            while (tables.next()) {
                log.debug("TABLE_NAME {}", tables.getString("TABLE_NAME"));
            }
        }


        ResultSet tableTypes = metaData.getTableTypes();

        if (tableTypes != null) {
            while (tableTypes.next()) {
                log.debug("TABLE_TYPE {}", tableTypes.getString("TABLE_TYPE"));
            }
        }
    }


}