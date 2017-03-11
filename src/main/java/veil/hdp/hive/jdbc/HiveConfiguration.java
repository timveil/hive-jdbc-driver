package veil.hdp.hive.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import veil.hdp.hive.jdbc.utils.JdbcUrlUtils;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveConfiguration {

    // constructor
    private final String url;

    // public getter & setter
    private String databaseName = "default";
    private int port = 10000;

    private String host;
    private List<URI> hosts = Lists.newArrayList();
    private final Map<String, String> sessionVariables = Maps.newHashMap();
    private final Map<String, String> hiveConfigurationParameters = Maps.newHashMap();
    private final Map<String, String> hiveVariables = Maps.newHashMap();
    private final Map<String, String> originalProperties = Maps.newHashMap();

    HiveConfiguration(String url, Properties info) {
        this.url = url;

        JdbcUrlUtils.parseUrl(url, info, this);
    }

    public String getUrl() {
        return url;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<URI> getHosts() {
        return hosts;
    }

    public void setHosts(List<URI> hosts) {
        this.hosts = hosts;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void addSessionVariables(Map<String, String> sessionVariables) {
        if (sessionVariables != null) {
            this.sessionVariables.putAll(sessionVariables);
        }
    }

    public void addSessionVariable(String key, String value) {
        this.sessionVariables.put(key, value);
    }

    public Map<String, String> getHiveConfigurationParameters() {
        return hiveConfigurationParameters;
    }

    public void addHiveConfigurationParameters(Map<String, String> hiveConfigurationParameters) {
        if (hiveConfigurationParameters != null) {
            this.hiveConfigurationParameters.putAll(hiveConfigurationParameters);
        }
    }

    public void addHiveConfigurationParameter(String key, String value) {
        this.hiveConfigurationParameters.put(key, value);
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public void addHiveVariables(Map<String, String> hiveVariables) {
        if (hiveVariables != null) {
            this.hiveVariables.putAll(hiveVariables);
        }
    }

    public void addHiveVariable(String key, String value) {
        this.hiveVariables.put(key, value);
    }

    public Map<String, String> getOriginalProperties() {
        return originalProperties;
    }

    public void addOriginalProperty(String key, String value) {
        this.originalProperties.put(key, value);
    }

    public String getUser() {
        return sessionVariables.get("user");
    }

    public String getPassword() {
        return sessionVariables.get("password");
    }

    public boolean isZookeeperDiscoverMode() {
        return sessionVariables.get("serviceDiscoveryMode") != null && sessionVariables.get("serviceDiscoveryMode").equalsIgnoreCase("zooKeeper");
    }

    public boolean isNoSasl() {
        return "noSasl".equals(sessionVariables.get("auth"));
    }

    @Override
    public String toString() {
        return "HiveConfiguration{" +
                "url='" + url + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", hosts=" + hosts +
                ", sessionVariables=" + sessionVariables +
                ", hiveConfigurationParameters=" + hiveConfigurationParameters +
                ", hiveVariables=" + hiveVariables +
                ", originalProperties=" + originalProperties +
                '}';
    }
}
