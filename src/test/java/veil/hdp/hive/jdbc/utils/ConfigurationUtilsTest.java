package veil.hdp.hive.jdbc.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import veil.hdp.hive.jdbc.BaseJunitTest;

import java.util.Properties;

public class ConfigurationUtilsTest extends BaseJunitTest {

    private String url = null;

    @Before
    public void setUp() throws Exception {
        url = "jdbc:hive2://foo:1,bar:2,baz:3/dbName;principal=hive/foo.bar.baz@HDP.LOCAL?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID";
    }

    @After
    public void tearDown() throws Exception {
        url = null;
    }

    @Test
    public void acceptURL() throws Exception {
        boolean accepts = ConfigurationUtils.acceptURL(url);
    }

    @Test
    public void buildConfiguration() throws Exception {


        Properties info = new Properties();
        info.put("user", "tveil");
        info.put("password", "tveil");
        info.put("randomkey", "randomvalue");
        info.put("hivevar:randhivevar", "yyyyyyy");
        info.put("hiveconf:randhiveconf", "xxxxxxx");


        HiveConfiguration hiveConfiguration = ConfigurationUtils.buildConfiguration(url, info);

    }

}