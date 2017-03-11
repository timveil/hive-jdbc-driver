package veil.hdp.hive.jdbc.utils;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveConfiguration {

    // constructor
    private final String url;
    private final String databaseName;
    private final List<URI> hosts;
    private Map<String, String> sessionVariables;
    private Map<String, String> hiveConfigurationParameters;
    private Map<String, String> hiveVariables;

    HiveConfiguration(String url, String databaseName, List<URI> hosts, Map<String, String> sessionVariables, Map<String, String> hiveConfigurationParameters, Map<String, String> hiveVariables) {
        this.url = url;
        this.databaseName = databaseName;
        this.hosts = hosts;
        this.sessionVariables = sessionVariables;
        this.hiveConfigurationParameters = hiveConfigurationParameters;
        this.hiveVariables = hiveVariables;
    }

    public String getUrl() {
        return url;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<URI> getHosts() {
        return hosts;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void addSessionVariable(String key, String value) {

        if (sessionVariables == null) {
            sessionVariables = new HashMap<>();
        }

        sessionVariables.put(key, value);
    }

    public Map<String, String> getHiveConfigurationParameters() {
        return hiveConfigurationParameters;
    }

    public void addHiveConfigurationParameter(String key, String value) {

        if (hiveConfigurationParameters == null) {
            hiveConfigurationParameters = new HashMap<>();
        }

        hiveConfigurationParameters.put(key, value);
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public void addHiveVariable(String key, String value) {

        if (hiveVariables == null) {
            hiveVariables = new HashMap<>();
        }

        hiveVariables.put(key, value);
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
                ", databaseName='" + databaseName + '\'' +
                ", hosts=" + hosts +
                ", sessionVariables=" + sessionVariables +
                ", hiveConfigurationParameters=" + hiveConfigurationParameters +
                ", hiveVariables=" + hiveVariables +
                '}';
    }
}
