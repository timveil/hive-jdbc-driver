package veil.hdp.hive.jdbc;

import org.apache.hadoop.hive.conf.HiveConf;

public enum HiveDriverStringProperty {
    HOST("host","",""),
    DATABASE_NAME("database", "", "default"),
    USER("user","", ""),
    PASSWORD("password","",""),
    //HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE: transport mode must match whats on the server.  should only be used to determine what kind of client to create, this isn't somthing that is "set" on the server.
    TRANSPORT_MODE("transport.mode", "", TransportMode.binary.toString()),

    // i think these need to be prefixed (like jdbc.) these should not be confused with actual hive conf parameters even thought they may result in the same value

    //HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION
    AUTHENTICATION_TYPE("authentication.type", "", ""),

    KERBEROS_PRINCIPAL("kerberos.principal","", ""),
    //https://community.hortonworks.com/questions/22154/hive-sasl-qop-setting-on-client-and-server.html
    // only applicable for kerberos
    //HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP
    KERBEROS_SASL_QOP("kerberos.sasl.qop","", ""),

    // only applicable in http mode
    SSL_TRUSTSTORE_PATH("ssl.truststore.path","", ""),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password", "", ""),
    SSL_KEYSTORE_PATH("ssl.keystore.path", "", ""),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password", "", ""),
    // only applicable in http mode
    COOKIE_NAME("cookie.name", "", "hive.server2.auth"),
    //HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE
    ZOOKEEPER_DISCOVERY_NAMESPACE("zookeeper.discovery.namespace", "", HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE.getDefaultValue()),

    ;


    private String name;
    private String defaultValue;
    private String description;

    HiveDriverStringProperty(String name, String description, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
