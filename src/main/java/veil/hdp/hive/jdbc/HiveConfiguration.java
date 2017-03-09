package veil.hdp.hive.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveConfiguration {

    private final String url;

    private String databaseName;
    private String host;
    private int port;
    private List<URI> hosts;
    private Map<String, String> sessionVariables;
    private Map<String, String> hiveConfigurationParameters;
    private Map<String, String> hiveVariables;
    private Map<String, String> connectionArguments;

    public HiveConfiguration(String url, Properties info) {
        this.url = url;

        this.connectionArguments = Maps.newHashMap();
        this.hosts = Lists.newArrayList();
        this.sessionVariables = Maps.newHashMap();
        this.hiveConfigurationParameters = Maps.newHashMap();
        this.hiveVariables = Maps.newHashMap();

        this.port = 10000;
        this.databaseName = "default";

        if (info != null) {
            for  (String name : info.stringPropertyNames()) {
                connectionArguments.put(name, info.getProperty(name));
            }
        }

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

    public Map<String, String> getHiveConfigurationParameters() {
        return hiveConfigurationParameters;
    }

    public void addHiveConfigurationParameters(Map<String, String> hiveConfigurationParameters) {
        if (hiveConfigurationParameters != null) {
            this.hiveConfigurationParameters.putAll(hiveConfigurationParameters);
        }
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public void addHiveVariables(Map<String, String> hiveVariables) {
        if (hiveVariables != null) {
            this.hiveVariables.putAll(hiveVariables);
        }
    }

    public Map<String, String> getConnectionArguments() {
        return connectionArguments;
    }

    public String getUser() {
        return connectionArguments.get("user");
    }

    public String getPassword() {
        return connectionArguments.get("password");
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
                ", connectionArguments=" + connectionArguments +
                '}';
    }
}
