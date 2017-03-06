package veil.hdp.hive.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class ConnectionParameters {

    private boolean embeddedMode;
    private String databaseName;
    private String host;
    private int port;
    private List<URI> hosts;
    private Map<String, String> sessionVariables;
    private Map<String, String> hiveConfigurationParameters;
    private Map<String, String> hiveVariables;

    public ConnectionParameters() {
        this.hosts = Lists.newArrayList();
        this.sessionVariables = Maps.newHashMap();
        this.hiveConfigurationParameters = Maps.newHashMap();
        this.hiveVariables = Maps.newHashMap();
        this.port = 10000;
        this.databaseName = "default";
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

    public boolean isEmbeddedMode() {
        return embeddedMode;
    }

    public void setEmbeddedMode(boolean embeddedMode) {
        this.embeddedMode = embeddedMode;
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

    public void setSessionVariables(Map<String, String> sessionVariables) {
        this.sessionVariables = sessionVariables;
    }

    public Map<String, String> getHiveConfigurationParameters() {
        return hiveConfigurationParameters;
    }

    public void setHiveConfigurationParameters(Map<String, String> hiveConfigurationParameters) {
        this.hiveConfigurationParameters = hiveConfigurationParameters;
    }

    public Map<String, String> getHiveVariables() {
        return hiveVariables;
    }

    public void setHiveVariables(Map<String, String> hiveVariables) {
        this.hiveVariables = hiveVariables;
    }

    public int getRetries() {
        return sessionVariables.get("retries") != null ? Integer.parseInt(sessionVariables.get("retries")) : 1;
    }

    public boolean isHttpTransportMode() {
        return sessionVariables.get("transportMode") != null && sessionVariables.get("transportMode").equalsIgnoreCase("http");
    }

    public boolean isSSL() {
        return sessionVariables.get("ssl") != null && sessionVariables.get("ssl").equalsIgnoreCase("true");
    }

    public boolean isCookieEnabled() {
        return sessionVariables.get("cookieAuth") == null || !sessionVariables.get("cookieAuth").equalsIgnoreCase("false");
    }

    public String getCookieName() {
        return sessionVariables.get("cookieName") != null ? sessionVariables.get("cookieName") : "hive.server2.auth";
    }

    public String getPrincipal() {
        return sessionVariables.get("principal");
    }

    public boolean isZookeeperDiscoverMode() {
        return sessionVariables.get("serviceDiscoveryMode") != null && sessionVariables.get("serviceDiscoveryMode").equalsIgnoreCase("zooKeeper");
    }

    public boolean isKerberos() {
        return !"noSasl".equals(sessionVariables.get("auth")) && sessionVariables.containsKey("principal");
    }

}
