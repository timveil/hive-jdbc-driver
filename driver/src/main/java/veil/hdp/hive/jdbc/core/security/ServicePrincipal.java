package veil.hdp.hive.jdbc.core.security;

public class ServicePrincipal {

    private final String service;
    private final String host;
    private final String realm;

    public ServicePrincipal(String service, String host, String realm) {
        this.service = service;
        this.host = host;
        this.realm = realm;
    }

    public String getService() {
        return service;
    }

    public String getHost() {
        return host;
    }

    public String getRealm() {
        return realm;
    }

    @Override
    public String toString() {
        return service + '/' + host + '@' + realm;
    }
}
