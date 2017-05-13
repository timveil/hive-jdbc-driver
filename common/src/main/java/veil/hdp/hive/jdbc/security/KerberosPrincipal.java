package veil.hdp.hive.jdbc.security;

public class KerberosPrincipal {

    private String user;
    private String server;
    private String realm;

    public KerberosPrincipal(String user, String server, String realm) {
        this.user = user;
        this.server = server;
        this.realm = realm;
    }

    public String getUser() {
        return user;
    }

    public String getServer() {
        return server;
    }

    public String getRealm() {
        return realm;
    }
}
