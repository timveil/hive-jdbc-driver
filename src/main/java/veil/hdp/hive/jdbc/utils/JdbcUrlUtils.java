package veil.hdp.hive.jdbc.utils;


import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveConfiguration;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcUrlUtils {

    private static final Logger log = LoggerFactory.getLogger(JdbcUrlUtils.class);

    private static final String JDBC_PART = "jdbc:";
    private static final String HIVE2_PART = "hive2:";
    private static final String JDBC_HIVE2_PREFIX = JDBC_PART + HIVE2_PART + "//";
    private static final String TMP_HOST = "tmp_host:00000";

    private static final String HIVE_VAR_PREFIX = "hivevar:";
    private static final String HIVE_CONF_PREFIX = "hiveconf:";

    public static boolean acceptURL(String url) {
        return url.startsWith(JDBC_HIVE2_PREFIX);
    }

    public static void parseUrl(String url, Properties info, HiveConfiguration hiveConfiguration) {

        String hostString = parseHostString(url);

        setHosts(hiveConfiguration, hostString);

        url = url.replace(hostString, TMP_HOST);

        URI tmpUri = URI.create(stripPrefix(JDBC_PART, url));

        String uriPath = getPath(tmpUri);

        String databaseName = parseDatabaseName(uriPath);
        hiveConfiguration.setDatabaseName(databaseName);

        Map<String, String> sessionVariables = parseSessionVariables(uriPath);
        hiveConfiguration.addSessionVariables(sessionVariables);

        String uriQuery = tmpUri.getQuery();

        Map<String, String> hiveConfigurationParameters = parseHiveConfigurationParameters(uriQuery);
        hiveConfiguration.addHiveConfigurationParameters(hiveConfigurationParameters);

        String uriFragment = tmpUri.getFragment();

        Map<String, String> hiveVariables = parseHiveVariables(uriFragment);
        hiveConfiguration.addHiveVariables(hiveVariables);

        if (!hiveConfiguration.getHosts().isEmpty()) {

            if (!hiveConfiguration.isZookeeperDiscoverMode()) {
                URI firstHost = hiveConfiguration.getHosts().get(0);
                hiveConfiguration.setHost(firstHost.getHost());
                hiveConfiguration.setPort(firstHost.getPort());
            } else {
                // todo
            }
        }

        overlayProperties(hiveConfiguration, info);

        if (log.isDebugEnabled()) {
            log.debug(hiveConfiguration.toString());
        }

    }

    private static void setHosts(HiveConfiguration hiveConfiguration, String hostString) {
        List<URI> uris = Lists.newArrayList();

        for (String host : Splitter.on(",").trimResults().omitEmptyStrings().split(hostString)) {
            uris.add(URI.create(HIVE2_PART + "//" + host));
        }

        hiveConfiguration.setHosts(uris);
    }

    private static Map<String, String> parseHiveConfigurationParameters(String query) {

        if (query != null) {
            return Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(query);
        }

        return null;

    }

    private static Map<String, String> parseHiveVariables(String fragment) {

        if (fragment != null) {
            return Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(fragment);
        }

        return null;

    }


    private static Map<String, String> parseSessionVariables(String path) {

        if (path != null && path.contains(";")) {
            path = path.substring(path.indexOf(';'));
            return Splitter.on(";").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(path);
        }

        return null;

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

    private static void overlayProperties(HiveConfiguration hiveConfiguration, Properties info) {

        if (info != null) {
            for (String key : info.stringPropertyNames()) {

                key = key.toLowerCase();

                hiveConfiguration.addOriginalProperty(key, info.getProperty(key));

                if (key.startsWith(HIVE_VAR_PREFIX)) {
                    String varKey = key.substring(HIVE_VAR_PREFIX.length());

                    if (hiveConfiguration.getHiveVariables().containsKey(varKey)) {
                        log.debug("hive variables map already contains key {}", varKey);
                    }

                    hiveConfiguration.addHiveVariable(varKey, info.getProperty(key));
                } else if (key.startsWith(HIVE_CONF_PREFIX)) {
                    String confKey = key.substring(HIVE_CONF_PREFIX.length());

                    if (hiveConfiguration.getHiveConfigurationParameters().containsKey(confKey)) {
                        log.debug("hive configuration parameters map already contains key {}", confKey);
                    }

                    hiveConfiguration.addHiveConfigurationParameter(confKey, info.getProperty(key));
                } else {

                    if (hiveConfiguration.getSessionVariables().containsKey(key)) {
                        log.debug("hive session variables map already contains key {}", key);
                    }

                    hiveConfiguration.addSessionVariable(key, info.getProperty(key));
                }

            }
        }

    }

}
