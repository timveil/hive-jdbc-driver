package veil.hdp.hive.jdbc.utils;


import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverBooleanProperty;
import veil.hdp.hive.jdbc.HiveDriverIntProperty;
import veil.hdp.hive.jdbc.HiveDriverPropertyInfo;
import veil.hdp.hive.jdbc.HiveDriverStringProperty;

import java.net.URI;
import java.nio.charset.Charset;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class DriverUtils {

    private static final Logger log = LoggerFactory.getLogger(DriverUtils.class);

    private static final String JDBC_PART = "jdbc:";
    private static final String HIVE2_PART = "hive2:";
    private static final String JDBC_HIVE2_PREFIX = JDBC_PART + HIVE2_PART + "//";


    public static boolean acceptURL(String url) {
        return url.startsWith(JDBC_HIVE2_PREFIX);
    }

    private static final HiveDriverPropertyInfo[] DRIVER_PROPERTIES = {
            new HiveDriverPropertyInfo(HiveDriverStringProperty.DATABASE_NAME.getName(),
                    HiveDriverStringProperty.DATABASE_NAME.getDescription(),
                    HiveDriverStringProperty.DATABASE_NAME.getDefaultValue(),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverStringProperty.HOST.getName(),
                    HiveDriverStringProperty.HOST.getDescription(),
                    HiveDriverStringProperty.HOST.getDefaultValue(),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.PORT_NUMBER.getName(),
                    HiveDriverIntProperty.PORT_NUMBER.getDescription(),
                    Integer.toString(HiveDriverIntProperty.PORT_NUMBER.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getName(),
                    HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getDescription(),
                    Integer.toString(HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.STATEMENT_QUERY_TIMEOUT.getName(),
                    HiveDriverIntProperty.STATEMENT_QUERY_TIMEOUT.getDescription(),
                    Integer.toString(HiveDriverIntProperty.STATEMENT_QUERY_TIMEOUT.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.STATEMENT_MAX_ROWS.getName(),
                    HiveDriverIntProperty.STATEMENT_MAX_ROWS.getDescription(),
                    Integer.toString(HiveDriverIntProperty.STATEMENT_MAX_ROWS.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.STATEMENT_FETCH_SIZE.getName(),
                    HiveDriverIntProperty.STATEMENT_FETCH_SIZE.getDescription(),
                    Integer.toString(HiveDriverIntProperty.STATEMENT_FETCH_SIZE.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverStringProperty.USERNAME.getName(),
                    HiveDriverStringProperty.USERNAME.getDescription(),
                    HiveDriverStringProperty.USERNAME.getDefaultValue(),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverBooleanProperty.ZOOKEEPER_DISCOVERY_ENABLED.getName(),
                    HiveDriverBooleanProperty.ZOOKEEPER_DISCOVERY_ENABLED.getDescription(),
                    Boolean.toString(HiveDriverBooleanProperty.ZOOKEEPER_DISCOVERY_ENABLED.getDefaultValue()),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverStringProperty.PASSWORD.getName(),
                    HiveDriverStringProperty.PASSWORD.getDescription(),
                    HiveDriverStringProperty.PASSWORD.getDefaultValue(),
                    false,
                    null)

    };


    public static Properties buildProperties(String url, Properties suppliedProperties) throws SQLException {

        Map<String, String> urlMap = normalize(parseUrl(url));
        Map<String, String> suppliedMap = normalize(convertPropertiesToMap(suppliedProperties));

        urlMap.putAll(suppliedMap);

        validateProperties(urlMap);

        return convertMapToProperties(urlMap);

    }

    public static DriverPropertyInfo[] buildDriverPropertyInfo(String url, Properties suppliedProperties) throws SQLException {
        Properties config = buildProperties(url, suppliedProperties);

        DriverPropertyInfo[] properties = new DriverPropertyInfo[DRIVER_PROPERTIES.length];

        for (int i = 0; i < DRIVER_PROPERTIES.length; i++) {
            properties[i] = DRIVER_PROPERTIES[i].build(config);
        }

        return properties;

    }


    private static Map<String, String> normalize(Map<String, String> properties) {

        Map<String, String> normalized = Maps.newHashMap();

        for (String key : properties.keySet()) {
            normalized.put(key.toLowerCase(), Strings.emptyToNull(properties.get(key)));
        }

        return normalized;

    }

    private static void validateProperties(Map<String, String> properties) throws SQLException {

        for (String key : properties.keySet()) {

            boolean found = false;

            for (HiveDriverPropertyInfo DRIVER_PROPERTY : DRIVER_PROPERTIES) {
                if (DRIVER_PROPERTY.getName().equals(key)) {
                    found = true;
                    break;
                }
            }

            if (HiveConf.getConfVars(key) != null) {

                found = true;

                String value = properties.get(key);

                String result = HiveConf.getConfVars(key).validate(value);

                if (result != null) {
                    throw new SQLException(result);
                }
            }

            if (!found) {
                log.warn("property [{}] is not a valid", key);
                //throw new SQLException("property [" + key + "] is not a valid property");
            }

        }

    }

    private static Properties convertMapToProperties(Map<String, String> map) {

        Properties properties = new Properties();

        for (String key : map.keySet()) {
            properties.setProperty(key, map.get(key));
        }

        return properties;

    }

    private static Map<String, String> convertPropertiesToMap(Properties properties) {
        Map<String, String> map = Maps.newHashMap();

        if (properties != null) {
            for (String property : properties.stringPropertyNames()) {
                map.put(property, properties.getProperty(property));
            }
        }

        return map;
    }

    private static Map<String, String> parseUrl(String url) throws SQLException {

        Map<String, String> properties = Maps.newHashMap();

        URI uri = URI.create(stripPrefix(JDBC_PART, url));

        String databaseName = Strings.emptyToNull(getDatabaseName(uri));

        properties.put(HiveDriverStringProperty.DATABASE_NAME.getName(), databaseName != null ? databaseName : HiveDriverStringProperty.DATABASE_NAME.getDefaultValue());

        String uriQuery = uri.getQuery();

        properties.putAll(parseQueryParameters(uriQuery));

        if (properties.containsKey(HiveDriverBooleanProperty.ZOOKEEPER_DISCOVERY_ENABLED.getName())) {

            String authority = uri.getAuthority();

            loadPropertiesFromZookeeper(authority,                    properties);

        } else {
            properties.put(HiveDriverStringProperty.HOST.getName(), uri.getHost());
            properties.put(HiveDriverIntProperty.PORT_NUMBER.getName(), Integer.toString(uri.getPort() != -1 ? uri.getPort() : HiveDriverIntProperty.PORT_NUMBER.getDefaultValue()));
        }

        return properties;
    }

    private static Map<String, String> parseQueryParameters(String path) {

        Map<String, String> parameters = Maps.newHashMap();

        if (path != null) {
            parameters.putAll(Splitter.on("&").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(path));
        }

        return parameters;

    }

    private static String getDatabaseName(URI uri) {
        String path = uri.getPath();

        if (path != null && path.startsWith("/")) {
            return path.replaceFirst("/", "");
        }

        return null;
    }


    private static String stripPrefix(String prefix, String url) {
        return url.replace(prefix, "").trim();
    }

    private static void loadPropertiesFromZookeeper(String authority, Map<String, String> properties) throws SQLException {

        String zooKeeperNamespace = properties.containsKey(HiveDriverStringProperty.ZOOKEEPER_DISCOVERY_NAMESPACE.getName())
                ? properties.get(HiveDriverStringProperty.ZOOKEEPER_DISCOVERY_NAMESPACE.getName())
                : HiveDriverStringProperty.ZOOKEEPER_DISCOVERY_NAMESPACE.getDefaultValue();
        int retry = properties.containsKey(HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getName())
                ? Integer.parseInt(properties.get(HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getName()))
                : HiveDriverIntProperty.ZOOKEEPER_DISCOVERY_RETRY.getDefaultValue();

        //hive.server2.authentication=NONE;hive.server2.transport.mode=binary;hive.server2.thrift.sasl.qop=auth;hive.server2.thrift.bind.host=hive-large.hdp.local;hive.server2.thrift.port=10000;hive.server2.use.SSL=false

        Random random = new Random();

        try (CuratorFramework zooKeeperClient = CuratorFrameworkFactory.builder().connectString(authority).retryPolicy(new RetryOneTime(retry)).build()) {

            zooKeeperClient.start();

            List<String> hosts = zooKeeperClient.getChildren().forPath("/" + zooKeeperNamespace);

            String randomHost = hosts.get(random.nextInt(hosts.size()));

            String hostData = new String(zooKeeperClient.getData().forPath("/" + zooKeeperNamespace + "/" + randomHost), Charset.forName("UTF-8"));

            Map<String, String> config = Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(hostData);

            for (String key : config.keySet()) {
                properties.put(key, config.get(key));
            }


        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

}
