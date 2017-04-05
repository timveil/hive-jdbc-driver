package test.java.veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import veil.hdp.hive.jdbc.utils.DriverUtils;

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

        String url = "jdbc:hive2://somehost:10000/test?transportMode=http&whoknows=me";

        Properties suppliedProperties = new Properties();
        suppliedProperties.setProperty("user", "hive");

        DriverPropertyInfo[] driverPropertyInfos = DriverUtils.buildDriverPropertyInfo(url, suppliedProperties);

        for (DriverPropertyInfo info : driverPropertyInfos) {
            log.debug("info.name [{}], info.value [{}]", info.name, info.value);
        }

    }

    @Test
    public void buildProperties() throws Exception {

        //String url =  "jdbc:hive2://host1:10001/test?statement.fetch.size=100&statement.max.rows=1";
        String url = "jdbc:hive2://hive.hdp.local:2181/default?zkEnabled=true&whoknows=me";

        Properties suppliedProperties = new Properties();
        suppliedProperties.setProperty("user", "hive");

        Properties properties = DriverUtils.buildProperties(url, suppliedProperties);

        log.debug(properties.toString());

    }


}