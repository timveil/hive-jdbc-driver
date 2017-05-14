package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.thrift.TProtocolVersion;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum HiveDriverProperty {

    // required by JDBC Spec
    HOST_NAME("host", null, true, null, "hive.server2.thrift.bind.host"),
    DATABASE_NAME("database", "default", true, null, null),

    // this is not currently used; need to perhaps merge with kerberos. when authmode == none then this is ignored
    USER("user", null, false, null, null),

    // this is not currently used; need to perhaps merge with kerberos. when authmode == none then this is ignored
    PASSWORD("password", null, false, null, null),

    PORT_NUMBER("port", "10000", true, null, "hive.server2.thrift.port"),

    // make sure to spell out differnces in readme; look at *.thrift
    THRIFT_PROTOCOL_VERSION("thriftVersion", Integer.toString(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10.getValue()), false, null, null),

    // not really used because i've separated into separate drivers
    TRANSPORT_MODE("transportMode", TransportMode.binary.toString(), false, null, "hive.server2.transport.mode", TransportMode.binary.toString(), TransportMode.http.toString()),

    // HTTP Mode related
    HTTP_SSL_ENABLED("sslEnabled", Boolean.FALSE.toString(), false, null, "hive.server2.use.ssl"),
    HTTP_ENDPOINT("endpoint", "cliservice", false, null, "hive.server2.thrift.http.path"),

    // zookeeper discovery related
    // not really used because i've separated into separate drivers
    ZOOKEEPER_DISCOVERY_ENABLED("zkEnabled", Boolean.FALSE.toString(), false, null, null),
    ZOOKEEPER_DISCOVERY_NAMESPACE("zkNamespace", "hiveserver2", false, null, null),
    ZOOKEEPER_DISCOVERY_RETRY("zkRetry", "1000", false, null, null),

    // principal passed to thrift server using TSaslClientTransport.  this is not the local principal
    KERBEROS_SERVER_PRINCIPAL("krb5ServerPrincipal", null, false, null, null),
    KERBEROS_LOCAL_PRINCIPAL("krb5LocalPrincipal", null, false, null, null),
    KERBEROS_LOCAL_KEYTAB("krb5LocalKeytab", null, false, null, null),
    // sun.security.krb5.debug
    KERBEROS_DEBUG_ENABLED("krb5Debug", "true", false, null, null),
    // javax.security.auth.useSubjectCredsOnly
    KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY("krb5SubjectOnly", "false", false, null, null),
    KERBEROS_PRE_AUTHENTICATION_ENABLED("krb5PreAuth", "false", false, null, null),

    // Sasl.QOP
    SASL_QUALITY_OF_PROTECTION("saslQOP", "auth-conf,auth-int,auth", false, null, null),
    // Sasl.SERVER_AUTH
    SASL_SERVER_AUTHENTICATION_ENABLED("saslAuth", "true", false, null, null),

    JAAS_DEBUG_ENABLED("jaasDebug", "true", false, null, null),

    AUTHENTICATION_MODE("authMode", AuthenticationMode.NONE.toString(), false, null, "hive.server2.authentication",
            AuthenticationMode.NONE.toString(),
            AuthenticationMode.NOSASL.toString(),
            AuthenticationMode.KERBEROS.toString(),
            AuthenticationMode.LDAP.toString(),
            AuthenticationMode.PAM.toString());

    private final String name;
    private final String defaultValue;
    private final boolean required;
    private final String description;
    private final String hiveConfName;
    private final String[] choices;


    HiveDriverProperty(String name, String defaultValue, boolean required, String description, String hiveConfName) {
        this(name, defaultValue, required, description, hiveConfName, (String[]) null);
    }

    HiveDriverProperty(String name, String defaultValue, boolean required, String description, String hiveConfName, String... choices) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.required = required;
        this.description = description;
        this.hiveConfName = hiveConfName;
        this.choices = choices;
    }

    public static HiveDriverProperty forAlias(String alias) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            if (property.hiveConfName != null && property.hiveConfName.equalsIgnoreCase(alias)) {
                return property;
            }
        }

        return null;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Properties properties) {
        if (defaultValue != null) {
            properties.setProperty(name, defaultValue);
        }
    }

    public boolean isRequired() {
        return required;
    }

    public String getDescription() {
        return description;
    }

    public String[] getChoices() {
        return choices;
    }

    public String getHiveConfName() {
        return hiveConfName;
    }

    public void set(Properties properties, String value) {
        if (value == null) {
            properties.remove(name);
        } else {
            properties.setProperty(name, value);
        }
    }

    public void set(Properties properties, boolean value) {
        properties.setProperty(name, Boolean.toString(value));
    }

    public void set(Properties properties, int value) {
        properties.setProperty(name, Integer.toString(value));
    }

    public String get(Properties properties) {
        return properties.getProperty(name, defaultValue);
    }

    public boolean getBoolean(Properties properties) {
        return Boolean.valueOf(get(properties));
    }

    public Integer getInteger(Properties properties) {
        String value = get(properties);

        if (value == null) {
            return null;
        }

        return Integer.parseInt(value);
    }

    public int getInt(Properties properties) {
        String value = get(properties);

        return Integer.parseInt(value);
    }

    public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
        propertyInfo.required = required;
        propertyInfo.description = description;
        propertyInfo.choices = choices;

        return propertyInfo;
    }
}
