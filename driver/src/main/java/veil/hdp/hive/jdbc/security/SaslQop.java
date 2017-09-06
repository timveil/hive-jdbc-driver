package veil.hdp.hive.jdbc.security;

public enum SaslQop {
    AUTH("auth"), AUTH_INT("auth-int"), AUTH_CONF("auth-conf");

    private final String value;

    SaslQop(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
