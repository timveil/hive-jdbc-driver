package veil.hdp.hive.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveConfiguration {

    // constructor
    private final String url;
    private final Properties connectionProperties;

    // public getter & setter
    private String databaseName;
    private List<URI> hosts = Lists.newArrayList();
    private final Map<String, String> sessionVariables = Maps.newHashMap();
    private final Map<String, String> hiveConfigurationParameters = Maps.newHashMap();
    private final Map<String, String> hiveVariables = Maps.newHashMap();

    public HiveConfiguration(String url, Properties connectionProperties) {
        this.url = url;
        this.connectionProperties = connectionProperties;
    }

    public String getUrl() {
        return url;
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

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public String getHost() {
        return hosts != null && !hosts.isEmpty() ? hosts.get(0).getHost() : null;
    }

    public int getPort() {
        return hosts != null && !hosts.isEmpty() ? hosts.get(0).getPort() : 10000;
    }

    public boolean hasMultipleHosts() {
        return hosts != null && !hosts.isEmpty() && hosts.size() > 1;
    }

    @Override
    public String toString() {
        return "HiveConfiguration{" +
                "url='" + url + '\'' +
                ", connectionProperties=" + connectionProperties +
                ", databaseName='" + databaseName + '\'' +
                ", hosts=" + hosts +
                ", sessionVariables=" + sessionVariables +
                ", hiveConfigurationParameters=" + hiveConfigurationParameters +
                ", hiveVariables=" + hiveVariables +
                '}';
    }
}
