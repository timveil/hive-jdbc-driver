package test.java.veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static java.lang.Class.forName;

public class HiveDriverTest extends BaseJunitTest {

    boolean connectBinary = true;

    private HiveDriver hiveDriver = null;

    @Before
    public void setUp() throws Exception {
        hiveDriver = new HiveDriver();
    }

    @After
    public void tearDown() throws Exception {
        hiveDriver = null;
    }

    @Test
    public void connect() throws Exception {


        forName("veil.hdp.hive.jdbc.HiveDriver");

        Connection connection = null;
        String url = null;

        Properties properties = new Properties();
        properties.setProperty("user", "hive");


        if (connectBinary) {

            log.debug("********** attempting binary connection");

            url = "jdbc:hive2://hive.hdp.local:10000/default";

            try {
                connection = hiveDriver.connect(url, properties);

                Assert.assertNotNull(connection);
            } finally {

                if (connection != null) {
                    connection.close();
                }
            }

        } else {

            log.debug("********** attempting http connection");

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