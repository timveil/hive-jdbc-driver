package veil.hdp.hive.jdbc.security;

public enum SaslQop {
    AUTH("auth"), AUTH_INT("auth-int"), AUTH_CONF("auth-conf");

    private String name;

    SaslQop(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
