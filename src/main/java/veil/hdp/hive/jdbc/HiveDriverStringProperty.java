package veil.hdp.hive.jdbc;

import org.apache.hadoop.hive.conf.HiveConf;

public enum HiveDriverStringProperty {
    //HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION
    AUTHENTICATION_TYPE("authentication.type", "", ""),
    //HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE
    TRANSPORT_MODE("transport.mode", "", ""),
    KERBEROS_PRINCIPAL("kerberos.principal","", ""),
    //https://community.hortonworks.com/questions/22154/hive-sasl-qop-setting-on-client-and-server.html
    // only applicable for kerberos
    //HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP
    KERBEROS_SASL_QOP("kerberos.sasl.qop","", ""),
    USERNAME("username","", ""),
    // only applicable in http mode
    SSL_TRUSTSTORE_PATH("ssl.truststore.path","", ""),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password", "", ""),
    SSL_KEYSTORE_PATH("ssl.keystore.path", "", ""),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password", "", ""),
    // only applicable in http mode
    COOKIE_NAME("cookie.name", "", "hive.server2.auth"),
    //HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE
    ZOOKEEPER_DISCOVERY_NAMESPACE("zookeeper.discovery.namespace", "", HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE.getDefaultValue()),
    DATABASE_NAME("database", "", "default"),
    PASSWORD("password","",""),
    HOST("host","","")
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
