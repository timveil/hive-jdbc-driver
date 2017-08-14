package veil.hdp.hive.jdbc.utils;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class PropertyUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertyUtils.class);

    private static PropertyUtils instance = null;
    private final Properties properties;


    private PropertyUtils() throws IOException {

        properties = new Properties();
        properties.load(getClass().getResourceAsStream("/driver-config.properties"));

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

    public static void printProperties(Properties properties) {
        StringBuilder builder = new StringBuilder("\n");
        builder.append(" -------------------------------------------------------------\n");

        List<String> strings = new ArrayList<>(properties.stringPropertyNames());

        Collections.sort(strings);

        for (String key : strings) {
            if (StringUtils.containsIgnoreCase(key, "password")) {
                continue;
            }

            builder.append('\t').append(key).append(" : ").append(properties.getProperty(key)).append('\n');
        }
        builder.append(" -------------------------------------------------------------\n");

        log.debug(builder.toString());
    }

    public String getValue(String key) {
        return properties.getProperty(key);
    }

    public int getIntValue(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public String getValue(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

}
