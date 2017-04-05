package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static java.lang.Class.forName;

public class BinaryDriverTest {


    private BinaryHiveDriver hiveDriver = null;

    @Before
    public void setUp() throws Exception {
        hiveDriver = new BinaryHiveDriver();
    }

    @After
    public void tearDown() throws Exception {
        hiveDriver = null;
    }

    @Test
    public void connect() throws Exception {


        forName("veil.hdp.hive.jdbc.BinaryHiveDriver");

        Connection connection = null;
        String url = null;

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        url = "jdbc:hive2://hive.hdp.local:10000/default";

        try {
            connection = hiveDriver.connect(url, properties);

            Assert.assertNotNull(connection);
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