package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUnitTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());


    public static String getHost() {
        return System.getProperty("test.host");
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "1000"));
    }

}


