package veil.hdp.hive.jdbc.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractDriverTest extends BaseTest {

    private Driver driver;

    public abstract Driver createDriver() throws SQLException;

    @BeforeEach
    public void setUp() throws Exception {
        driver = createDriver();
    }

    @AfterEach
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
        properties.setProperty("httpPath", "xxxx");
        properties.setProperty("sslTrustStore", "dummy store");
        properties.setProperty("zooKeeperNamespace", "zzzzzz");

        url = "jdbc:hive2://" + getHost() + ":10000/default";

        try {
            connection = driver.connect(url, properties);

            assertNotNull(connection);
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

        assertTrue(acceptsURL);

        url = "jdbc:mysql://foo";

        acceptsURL = driver.acceptsURL(url);

        assertFalse(acceptsURL);
    }
}
