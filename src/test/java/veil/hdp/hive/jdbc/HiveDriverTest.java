package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static java.lang.Class.forName;
import static java.sql.DriverManager.getConnection;

public class HiveDriverTest extends BaseJunitTest {
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void connect() throws Exception {
        forName("veil.hdp.hive.jdbc.HiveDriver");

        String url = "jdbc:hive2://hive.hdp.local:10000/default?transport.mode=binary";

        Connection connection = getConnection(url, "hive", "dummy");

        Assert.assertNotNull(connection);

        connection.close();

    }

    @Test
    public void acceptsURL() throws Exception {

    }

}