package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import veil.hdp.hive.jdbc.test.BaseUnitTest;
import veil.hdp.hive.jdbc.utils.DriverUtils;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public class DriverUtilsTest extends BaseUnitTest {


    private Properties suppliedProperties;
    private String url;


    @Before
    public void setUp() throws Exception {

        url = "jdbc:hive2://somehost:10000/test?transportMode=http&whoknows=me";

        suppliedProperties = new Properties();
        suppliedProperties.setProperty("user", "hive");
    }

    @After
    public void tearDown() throws Exception {

        url = null;
        suppliedProperties = null;
    }

    @Test
    public void acceptURL() throws Exception {
        String url = null;

        boolean accepts = DriverUtils.acceptURL(url);
    }

    @Test
    public void buildDriverPropertyInfo() throws Exception {


        DriverPropertyInfo[] driverPropertyInfos = DriverUtils.buildDriverPropertyInfo(url, suppliedProperties, (properties, uri) -> {
            HiveDriverProperty.HOST_NAME.set(properties, uri.getHost());
        });

        for (DriverPropertyInfo info : driverPropertyInfos) {
            log.debug("info.name [{}], info.value [{}]", info.name, info.value);
        }

    }

    @Test
    public void buildProperties() throws Exception {


        Properties newProperties = DriverUtils.buildProperties(url, suppliedProperties, (properties, uri) -> {
            HiveDriverProperty.HOST_NAME.set(properties, uri.getHost());
        });

        log.debug(newProperties.toString());

    }


}