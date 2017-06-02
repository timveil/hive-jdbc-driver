package veil.hdp.hive.jdbc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUnitTest {

    protected static final Logger log = LoggerFactory.getLogger(BaseUnitTest.class);


    public static String getHost() {
        return System.getProperty("test.host");
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "100"));
    }

}


