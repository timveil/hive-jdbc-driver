package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class QueryUtilsTest extends BaseJunitTest {

    HiveConnection connection = null;

    @Before
    public void setUp() throws Exception {


        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default?transport.mode=binary";

        connection = (HiveConnection) new HiveDriver().connect(url, properties);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void fetchResults() throws Exception {

    }

    @Test
    public void fetchLogs() throws Exception {

    }

    @Test
    public void closeOperation() throws Exception {

    }

    @Test
    public void cancelOperation() throws Exception {

    }

    @Test
    public void closeSession() throws Exception {

    }

    @Test
    public void executeSql() throws Exception {

    }

    @Test
    public void waitForStatementToComplete() throws Exception {

    }

    @Test
    public void openSession() throws Exception {

    }

    @Test
    public void getSchema() throws Exception {
    }



}