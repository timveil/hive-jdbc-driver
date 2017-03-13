package veil.hdp.hive.jdbc.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import veil.hdp.hive.jdbc.BaseJunitTest;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public class DriverUtilsTest extends BaseJunitTest {


    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void acceptURL() throws Exception {
        String url = null;

        boolean accepts = DriverUtils.acceptURL(url);
    }

    @Test
    public void buildDriverPropertyInfo() throws Exception {

        String url =  "jdbc:hive2://host:10000/test?statement.fetch.size=100";

        Properties suppliedProperties = new Properties();
        suppliedProperties.setProperty("username", "hive");

        DriverPropertyInfo[] driverPropertyInfos = DriverUtils.buildDriverPropertyInfo(url, suppliedProperties);

        System.out.println(driverPropertyInfos);
    }

    @Test
    public void buildProperties() throws Exception {

        String url =  "jdbc:hive2://host1:10001/test?statement.fetch.size=100&statement.max.rows=1";

        Properties suppliedProperties = new Properties();
        suppliedProperties.setProperty("username", "hive");

        Properties properties = DriverUtils.buildProperties(url, suppliedProperties);

        log.debug(properties.toString());

    }



}