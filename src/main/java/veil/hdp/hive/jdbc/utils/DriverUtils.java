package veil.hdp.hive.jdbc.utils;


import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverBooleanProperty;
import veil.hdp.hive.jdbc.HiveDriverIntProperty;
import veil.hdp.hive.jdbc.HiveDriverPropertyInfo;
import veil.hdp.hive.jdbc.HiveDriverStringProperty;

import java.net.URI;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DriverUtils {

    private static final Logger log = LoggerFactory.getLogger(DriverUtils.class);

    private static final String JDBC_PART = "jdbc:";
    private static final String HIVE2_PART = "hive2:";
    private static final String JDBC_HIVE2_PREFIX = JDBC_PART + HIVE2_PART + "//";
    private static final String TMP_HOST = "tmp_host:00000";


    public static boolean acceptURL(String url) {
        return url.startsWith(JDBC_HIVE2_PREFIX);
    }

    private static final HiveDriverPropertyInfo[] DRIVER_PROPERTIES = {
            new HiveDriverPropertyInfo(HiveDriverStringProperty.DATABASE_NAME.getName(),
                    HiveDriverStringProperty.DATABASE_NAME.getDescription(),
                    HiveDriverStringProperty.DATABASE_NAME.getDefaultValue(),
                    false,
                    null),
            new HiveDriverPropertyInfo(HiveDriverIntProperty.PORT_NUMBER.getName(),
                    HiveDriverIntProperty.PORT_NUMBER.getDescription(),
                    Integer.toString(HiveDriverIntProperty.PORT_NUMBER.getDefaultValue()),
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
            new HiveDriverPropertyInfo(HiveDriverStringProperty.PASSWORD.getName(),
                    HiveDriverStringProperty.PASSWORD.getDescription(),
                    HiveDriverStringProperty.PASSWORD.getDefaultValue(),
                    false,
                    null)

    };


    public static Properties buildProperties(String url, Properties suppliedProperties) throws SQLException {
        //pull property names from url
        //normailize names ie lower
        //fix synonyms - forget this; no synonyms
        //check names against driver properties + hiveconf
        //warn or error if not found
        //parse hive variables variables

        Map<String, String> urlMap = normalizeKeys(parseUrl(url));
        Map<String, String> suppliedMap = normalizeKeys(convertPropertiesToMap(suppliedProperties));

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


    private static Map<String, String> normalizeKeys(Map<String, String> properties) {

        Map<String, String> normalized = Maps.newHashMap();

        for(String key : properties.keySet()) {
            normalized.put(key.toLowerCase(), properties.get(key));
        }

        return normalized;

    }

    private static void validateProperties(Map<String, String> properties) throws SQLException {

        for(String key : properties.keySet()) {

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
                throw new SQLException("property [" + key + "] is not a valid property");
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

    private static Map<String, String> parseUrl(String url) {

        Map<String, String> properties = Maps.newHashMap();

        String tmpUrl = url;

        String hostString = parseHostString(tmpUrl);

        List<URI> hosts = getHosts(hostString);

        tmpUrl = tmpUrl.replace(hostString, TMP_HOST);

        URI tmpUri = URI.create(stripPrefix(JDBC_PART, tmpUrl));

        String uriPath = getPath(tmpUri);

        String databaseName = parseDatabaseName(uriPath);

        properties.put(HiveDriverStringProperty.DATABASE_NAME.getName(), databaseName);

        properties.putAll(parseSessionVariables(uriPath));

        if (hosts.size() > 1 && properties.containsKey(HiveDriverBooleanProperty.ZOOKEEPER_DISCOVERY_ENABLED.getName())) {
            // fetch host from zookeeper
        } else {
            URI firstHost = hosts.get(0);
            properties.put(HiveDriverStringProperty.HOST.getName(), firstHost.getHost());
            properties.put(HiveDriverIntProperty.PORT_NUMBER.getName(), Integer.toString(firstHost.getPort()));
        }

        return properties;
    }

    private static List<URI> getHosts(String hostString) {
        List<URI> uris = Lists.newArrayList();

        for (String host : Splitter.on(",").trimResults().omitEmptyStrings().split(hostString)) {
            uris.add(URI.create(HIVE2_PART + "//" + host));
        }

        return uris;
    }

    private static Map<String, String> parseSessionVariables(String path) {

        Map<String, String> parameters = Maps.newHashMap();

        if (path != null && path.contains(";")) {
            path = path.substring(path.indexOf(';'));
            parameters.putAll(Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(path));
        }

        return parameters;

    }

    private static String parseDatabaseName(String path) {

        if (path != null && path.contains(";")) {
            return path.substring(0, path.indexOf(';'));
        } else {
            return path;
        }
    }

    private static String getPath(URI uri) {
        String path = uri.getPath();

        if (path != null && path.startsWith("/")) {
            return path.replaceFirst("/", "");
        }

        return null;
    }

    private static String parseHostString(String url) {

        url = stripPrefix(JDBC_HIVE2_PREFIX, url);

        return url.substring(0, CharMatcher.anyOf("/?#").indexIn(url));
    }

    private static String stripPrefix(String prefix, String url) {
        return url.replace(prefix, "").trim();
    }

}
