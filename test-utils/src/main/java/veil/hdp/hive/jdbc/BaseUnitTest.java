package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUnitTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());


    public String getHost() {
        return System.getProperty("test.host");
    }

}


