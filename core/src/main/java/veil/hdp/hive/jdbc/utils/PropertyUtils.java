package veil.hdp.hive.jdbc.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class PropertyUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertyUtils.class);

    private static PropertyUtils instance = null;
    private Properties properties;


    private PropertyUtils() throws IOException {

        properties = new Properties();
        properties.load(getClass().getResourceAsStream("/driver-version.properties"));

    }

    public static PropertyUtils getInstance() {
        if (instance == null) {
            try {
                instance = new PropertyUtils();
            } catch (IOException ioe) {
                log.error(ioe.getMessage(), ioe);
            }
        }
        return instance;
    }

    public String getValue(String key) {
        return properties.getProperty(key);
    }

    public String getValue(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
