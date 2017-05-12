package veil.hdp.hive.jdbc.utils;

public class Principal {

    private String user;
    private String server;
    private String realm;

    public Principal(String user, String server, String realm) {
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
