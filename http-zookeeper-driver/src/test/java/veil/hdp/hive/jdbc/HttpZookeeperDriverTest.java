package veil.hdp.hive.jdbc;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;


public class HttpZookeeperDriverTest {


    private HttpZookeeperHiveDriver hiveDriver = null;

    @Before
    public void setUp() throws Exception {
        hiveDriver = new HttpZookeeperHiveDriver();
    }

    @After
    public void tearDown() throws Exception {
        hiveDriver = null;
    }

    @Test
    public void connect() throws Exception {

        Connection connection = null;
        String url = null;

        Properties properties = new Properties();
        properties.setProperty("user", "hive");


        url = "jdbc:hive2://hive.hdp.local:10001/default?transportMode=http";

        try {
            connection = hiveDriver.connect(url, properties);

            Assert.assertNotNull(connection);

            connection.close();
        } finally {

            if (connection != null) {
                connection.close();
            }
        }

    }

    @Test
    public void acceptsURL() throws Exception {
        String url = "jdbc:hive2://foo";

        boolean acceptsURL = hiveDriver.acceptsURL(url);

        Assert.assertTrue(acceptsURL);

        url = "jdbc:mysql://foo";

        acceptsURL = hiveDriver.acceptsURL(url);

        Assert.assertFalse(acceptsURL);
    }

}