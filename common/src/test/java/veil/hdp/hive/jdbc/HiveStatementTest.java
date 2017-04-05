package test.java.veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by tveil on 3/17/17.
 */
public class HiveStatementTest extends BaseJunitTest {

    Connection connection = null;

    Statement statement = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive.hdp.local:10000/default?transport.mode=binary";

        connection = new HiveDriver().connect(url, properties);

        statement = connection.createStatement();

    }

    @After
    public void tearDown() throws Exception {

        if (statement != null) {
            statement.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void execute() throws Exception {

        boolean execute = statement.execute("select * from ey_tax_poc.anek_orc");

        ResultSet resultSet = statement.getResultSet();

        while (resultSet.next()) {
            log.debug("col 1 {}", resultSet.getString(1));
        }

        resultSet.close();
    }

}