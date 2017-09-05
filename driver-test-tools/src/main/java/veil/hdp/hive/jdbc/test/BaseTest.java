package veil.hdp.hive.jdbc.test;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class BaseTest {

    protected static final Logger log = LogManager.getLogger(BaseTest.class);

    public BaseTest() {
        java.util.logging.Logger.getLogger("javax.security.sasl").setLevel(java.util.logging.Level.FINEST);
    }

    public static String getHost() {
        String host = System.getProperty("test.host", "jdbc-binary.hdp.local");

        if (host == null) {
            throw new IllegalArgumentException("can't run test without a valid host.  You must set the property \"test.host\" before executing test.  For example: -Dtest.host=jdbc-binary.hdp.local");
        }

        return host;
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "10"));
    }

}


