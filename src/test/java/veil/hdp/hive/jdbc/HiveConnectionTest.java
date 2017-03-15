package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Properties;

public class HiveConnectionTest extends BaseJunitTest {

    Connection connection = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String  url = "jdbc:hive2://hive.hdp.local:10000/default";

        connection = new HiveDriver().connect(url, properties);

    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void getMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: {}", metaData.getDriverVersion());
        log.debug("database product version: {}", metaData.getDatabaseProductVersion());
    }




}