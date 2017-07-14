package veil.hdp.hive.jdbc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;



public class BaseUnitTest {

    protected static final Logger log = LoggerFactory.getLogger(BaseUnitTest.class);

    public BaseUnitTest() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        java.util.logging.Logger.getLogger("javax.security.sasl").setLevel(java.util.logging.Level.FINEST);


    }

    public static String getHost() {
        return System.getProperty("test.host");
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "100"));
    }

}


