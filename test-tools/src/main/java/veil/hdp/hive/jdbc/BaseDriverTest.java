package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public abstract class BaseDriverTest extends BaseUnitTest {

    private Driver driver;

    public abstract Driver createDriver() throws SQLException;

    @Before
    public void setUp() throws Exception {
        driver = createDriver();
    }

    @After
    public void tearDown() throws Exception {
        if (driver != null) {
            driver = null;
        }
    }

    @Test
    public void connect() throws Exception {

        Connection connection = null;
        String url;

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        url = "jdbc:hive2://" + getHost() + ":10000/default";

        try {
            connection = driver.connect(url, properties);

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

        boolean acceptsURL = driver.acceptsURL(url);

        Assert.assertTrue(acceptsURL);

        url = "jdbc:mysql://foo";

        acceptsURL = driver.acceptsURL(url);

        Assert.assertFalse(acceptsURL);
    }
}
