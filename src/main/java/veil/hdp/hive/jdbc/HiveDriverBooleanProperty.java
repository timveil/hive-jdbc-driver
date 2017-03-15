package veil.hdp.hive.jdbc;

public enum HiveDriverBooleanProperty {
    SSL_ENABLED("ssl.enabled", "", false),
    KERBEROS_PRE_AUTHENTICATED_SUBJECT_ENABLED("kerberos.subject.enabled", "", false),
    ZOOKEEPER_DISCOVERY_ENABLED("zookeeper.discovery.enabled", "", false),
    COOKIE_REPLAY_ENABLED("cookie.replay.enabled", "", true),
    ;

    private final String name;
    private final boolean defaultValue;
    private final String description;

    HiveDriverBooleanProperty(String name, String description, boolean defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public boolean getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
