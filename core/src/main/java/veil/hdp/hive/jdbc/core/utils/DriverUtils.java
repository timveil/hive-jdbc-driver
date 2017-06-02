package veil.hdp.hive.jdbc.core.utils;


import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.HiveDriverProperty;
import veil.hdp.hive.jdbc.core.HiveSQLException;
import veil.hdp.hive.jdbc.core.PropertiesCallback;

import java.net.URI;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DriverUtils {

    private static final Logger log = LoggerFactory.getLogger(DriverUtils.class);

    private static final String JDBC_PART = "jdbc:";
    private static final String HIVE2_PART = "hive2:";
    private static final String JDBC_HIVE2_PREFIX = JDBC_PART + HIVE2_PART + "//";
    private static final Pattern FORWARD_SLASH_PATTERN = Pattern.compile("/");
    private static final Pattern JDBC_PATTERN = Pattern.compile(DriverUtils.JDBC_PART, Pattern.LITERAL);

    public static boolean acceptURL(String url) {
        return url.startsWith(JDBC_HIVE2_PREFIX);
    }


    public static Properties buildProperties(String url, Properties suppliedProperties, PropertiesCallback callback) throws SQLException {

        Properties properties = new Properties();

        loadDefaultProperties(properties);

        loadSuppliedProperties(suppliedProperties, properties);

        parseUrl(url, properties, callback);

        validateProperties(properties);

        printProperties(properties);

        return properties;

    }

    private static void loadSuppliedProperties(Properties suppliedProperties, Properties properties) {
        for (String key : suppliedProperties.stringPropertyNames()) {

            String value = StringUtils.trimToNull(suppliedProperties.getProperty(key));

            if (value != null) {
                properties.setProperty(key, value);
            }

        }
    }

    private static void loadDefaultProperties(Properties properties) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            property.setDefaultValue(properties);
        }
    }

    private static void printProperties(Properties properties) {
        StringBuilder builder = new StringBuilder("\n******************************************\n");
        builder.append("connection properties\n");
        builder.append("******************************************\n");

        List<String> strings = new ArrayList<>(properties.stringPropertyNames());

        Collections.sort(strings);

        for (String key : strings) {
            if (key.equalsIgnoreCase("password")) {
                continue;
            }

            builder.append('\t').append(key).append(" : ").append(properties.getProperty(key)).append('\n');
        }
        builder.append("******************************************\n");

        log.debug(builder.toString());
    }

    public static DriverPropertyInfo[] buildDriverPropertyInfo(String url, Properties suppliedProperties, PropertiesCallback callback) throws SQLException {
        Properties properties = buildProperties(url, suppliedProperties, callback);

        HiveDriverProperty[] driverProperties = HiveDriverProperty.values();

        DriverPropertyInfo[] driverPropertyInfoArray = new DriverPropertyInfo[driverProperties.length];

        for (int i = 0; i < driverPropertyInfoArray.length; i++) {
            driverPropertyInfoArray[i] = driverProperties[i].toDriverPropertyInfo(properties);
        }

        return driverPropertyInfoArray;

    }


    private static void validateProperties(Properties properties) throws SQLException {

        for (String key : properties.stringPropertyNames()) {

            boolean valid = false;

            for (HiveDriverProperty property : HiveDriverProperty.values()) {
                if (property.getKey().equalsIgnoreCase(key)) {
                    valid = true;
                    break;
                }
            }

            if (key.startsWith("hive.")) {
                valid = true;
            }

            if (!valid) {
                log.warn("property [{}] is not valid", key);
            }

        }

        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            if (property.isRequired() && property.get(properties) == null) {
                throw new HiveSQLException(MessageFormat.format("property [{0}] is required", property.getKey()));
            }
        }

    }


    private static void parseUrl(String url, Properties properties, PropertiesCallback callback) {

        URI uri = URI.create(stripPrefix(url));

        String databaseName = StringUtils.trimToNull(getDatabaseName(uri));

        HiveDriverProperty.DATABASE_NAME.set(properties, databaseName);

        String uriQuery = uri.getQuery();

        parseQueryParameters(uriQuery, properties);

        callback.merge(properties, uri);

    }

    private static void parseQueryParameters(String path, Properties properties) {

        Map<String, String> parameters = new HashMap<>();

        if (path != null) {
            parameters.putAll(Splitter.on("&").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(path));
        }

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String value = StringUtils.trimToNull(entry.getValue());

            if (value != null) {
                properties.setProperty(entry.getKey(), value);
            }
        }

    }

    private static String getDatabaseName(URI uri) {
        String path = uri.getPath();

        if (path != null && path.startsWith("/")) {
            return FORWARD_SLASH_PATTERN.matcher(path).replaceFirst("");
        }

        return null;
    }


    private static String stripPrefix(String url) {
        return JDBC_PATTERN.matcher(url).replaceAll(Matcher.quoteReplacement(""));

    }


}
