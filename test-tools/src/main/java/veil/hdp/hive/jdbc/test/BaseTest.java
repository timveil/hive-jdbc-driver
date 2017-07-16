package veil.hdp.hive.jdbc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;



public class BaseTest {

    protected static final Logger log = LoggerFactory.getLogger(BaseTest.class);

    public BaseTest() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        java.util.logging.Logger.getLogger("javax.security.sasl").setLevel(java.util.logging.Level.FINEST);
    }

    public static String getHost() {
        String host = System.getProperty("test.host");

        if (host == null) {
            throw new IllegalArgumentException("can't run test without a valid host.  You must set the property \"test.host\" before executing test.  For example: -Dtest.host=hdp1.lab.local");
        }

        return host;
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "100"));
    }

}


