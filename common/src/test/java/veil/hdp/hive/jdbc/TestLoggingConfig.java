package test.java.veil.hdp.hive.jdbc;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TestLoggingConfig {

    // for JUL
    //       -Djava.util.logging.config.class=veil.hdp.hive.jdbc.TestLoggingConfig

    public TestLoggingConfig() {
        try {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] (%2$s) %5$s %6$s%n");

            final ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.FINEST);
            consoleHandler.setFormatter(new SimpleFormatter());

            final Logger driver = Logger.getLogger("veil.hdp");
            driver.setLevel(Level.FINEST);
            driver.addHandler(consoleHandler);

            final Logger apache = Logger.getLogger("org.apache");
            apache.setLevel(Level.FINEST);
            apache.addHandler(consoleHandler);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
