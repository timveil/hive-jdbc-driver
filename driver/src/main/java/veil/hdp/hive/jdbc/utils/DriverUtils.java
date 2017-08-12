package veil.hdp.hive.jdbc.utils;


import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveSQLException;

import java.net.URI;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DriverUtils {

    private static final Logger log = LoggerFactory.getLogger(DriverUtils.class);

    private static final String JDBC_PART = "jdbc:";
    private static final String HIVE2_PART = "hive2:";
    private static final String JDBC_HIVE2_PREFIX = JDBC_PART + HIVE2_PART + "//";
    private static final Pattern FORWARD_SLASH_PATTERN = Pattern.compile("/");
    private static final Pattern JDBC_PATTERN = Pattern.compile(DriverUtils.JDBC_PART, Pattern.LITERAL);

    public static boolean acceptURL(String url) throws SQLException {

        if (url == null) {
            throw new HiveSQLException("url is null");
        }

        return url.startsWith(JDBC_HIVE2_PREFIX);
    }

    public static String buildUrl(Properties properties) {
        return JDBC_HIVE2_PREFIX + HiveDriverProperty.HOST_NAME.get(properties) + ':' + HiveDriverProperty.PORT_NUMBER.getInt(properties) + '/' + HiveDriverProperty.DATABASE_NAME.get(properties);
    }


    public static Properties buildProperties(String url, Properties suppliedProperties) throws SQLException {

        Properties properties = new Properties();

        // load defaults
        loadDefaultProperties(properties);

        // loads properties supplied by the JDBC api Driver.connect method
        loadSuppliedProperties(suppliedProperties, properties);

        // parse the url supplied by the JDBC api Driver.connect method
        parseUrl(url, properties);

        return properties;

    }


    private static String normalizeKey(String key) {

        if (key == null) {
            throw new RuntimeException("key is null");
        }

        HiveDriverProperty property = HiveDriverProperty.forKeyIgnoreCase(key);

        if (property != null) {
            return property.getKey();
        }

        return key;
    }


    private static void loadSuppliedProperties(Properties suppliedProperties, Properties properties) {
        for (String key : suppliedProperties.stringPropertyNames()) {

            String value = StringUtils.trimToNull(suppliedProperties.getProperty(key));

            if (value != null) {
                properties.setProperty(normalizeKey(key), value);
            }

        }
    }

    private static void loadDefaultProperties(Properties properties) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            property.setDefaultValue(properties);
        }
    }

    public static DriverPropertyInfo[] buildDriverPropertyInfo(String url, Properties suppliedProperties) throws SQLException {
        Properties properties = buildProperties(url, suppliedProperties);

        HiveDriverProperty[] driverProperties = HiveDriverProperty.values();

        DriverPropertyInfo[] driverPropertyInfoArray = new DriverPropertyInfo[driverProperties.length];

        for (int i = 0; i < driverPropertyInfoArray.length; i++) {
            driverPropertyInfoArray[i] = driverProperties[i].toDriverPropertyInfo(properties);
        }

        return driverPropertyInfoArray;

    }


    private static void validateProperties(Properties properties) {

        for (String key : properties.stringPropertyNames()) {

            boolean valid = false;

            if (HiveDriverProperty.forKeyIgnoreCase(key) != null) {
                valid = true;
            }

            if (key.startsWith("hive.")) {
                valid = true;
            }

            if (!valid) {
                log.warn("property [{}] is unknown and possibly invalid.  This could be a configuration issue (ie. wrong case, misspelling, etc.) or a custom property.  It is wise to double check this key/value pair", key);
            }

        }

    }


    private static void parseUrl(String url, Properties properties) throws SQLException {

        URI uri = URI.create(stripPrefix(url));

        String databaseName = StringUtils.trimToNull(getDatabaseName(uri));

        HiveDriverProperty.DATABASE_NAME.set(properties, databaseName);

        String uriQuery = uri.getQuery();

        parseQueryParameters(uriQuery, properties);

        boolean zookeeperDiscoveryEnabled = HiveDriverProperty.ZOOKEEPER_DISCOVERY_ENABLED.getBoolean(properties);

        if (zookeeperDiscoveryEnabled) {

            String authority = uri.getAuthority();

            ZookeeperUtils.loadPropertiesFromZookeeper(authority, properties);

        } else {

            HiveDriverProperty.HOST_NAME.set(properties, uri.getHost());

            if (uri.getPort() != -1) {
                HiveDriverProperty.PORT_NUMBER.set(properties, uri.getPort());
            }
        }

        validateProperties(properties);

    }

    private static void parseQueryParameters(String uriQuery, Properties properties) {

        Map<String, String> parameters = new HashMap<>();

        if (uriQuery != null) {
            parameters.putAll(Splitter.on("&").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(uriQuery));
        }

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String value = StringUtils.trimToNull(entry.getValue());

            if (value != null) {
                properties.setProperty(normalizeKey(entry.getKey()), value);
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
