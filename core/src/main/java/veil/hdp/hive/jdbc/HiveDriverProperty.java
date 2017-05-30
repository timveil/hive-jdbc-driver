package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.security.KerberosMode;
import veil.hdp.hive.jdbc.thrift.TProtocolVersion;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public enum HiveDriverProperty {

    /***************************************************
     *  COMMON
     ***************************************************/
    // required by JDBC Spec
    HOST_NAME("host", null, true, null, "hive.server2.thrift.bind.host"),
    DATABASE_NAME("database", "default", true, null, null),

    // when AUTHENTICATION_MODE == NONE then this is ignored; can be principal when using KERBEROS_MODE = PASSWORD
    USER("user", null, false, null, null),

    // when AUTHENTICATION_MODE == NONE then this is ignored
    PASSWORD("password", null, false, null, null),

    PORT_NUMBER("port", "10000", true, null, "hive.server2.thrift.port"),

    // should not be used in URL because its coded into separate drivers
    TRANSPORT_MODE("transportMode", TransportMode.binary.name(), false, null, "hive.server2.transport.mode", TransportMode.binary.name(), TransportMode.http.name()),

    AUTHENTICATION_MODE("authMode", AuthenticationMode.NONE.name(), false, null, "hive.server2.authentication",
            AuthenticationMode.NONE.name(),
            AuthenticationMode.NOSASL.name(),
            AuthenticationMode.KERBEROS.name(),
            AuthenticationMode.LDAP.name(),
            AuthenticationMode.PAM.name()),

    /***************************************************
     *  BINARY
     ***************************************************/

    // make sure to spell out differences in readme; look at *.thrift
    THRIFT_PROTOCOL_VERSION("thriftVersion", Integer.toString(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10.getValue()), false, null, null),


    /***************************************************
     *  HTTP
     ***************************************************/

    HTTP_SSL_ENABLED("httpsEnabled", Boolean.FALSE.toString(), false, null, "hive.server2.use.ssl"),
    HTTP_ENDPOINT("httpEndpoint", "cliservice", false, null, "hive.server2.thrift.http.path"),

    HTTP_POOL_ENABLED("httpPoolEnabled", "false", false, null, null),
    HTTP_POOL_MAX_TOTAL("httpPoolMax", "100", false, null, null),
    HTTP_POOL_MAX_PER_ROUTE("httpPoolMaxRoute", "20", false, null, null),

    // https://issues.apache.org/jira/browse/HIVE-9709
    HTTP_COOKIE_REPLAY_ENABLED("httpCookieReplayEnabled", Boolean.TRUE.toString(), false, null, null),

    // todo: this seems pretty hardcoded on the server side.  not sure how/why this would ever change
    HTTP_COOKIE_NAME("httpCookieName", "hive.server2.auth", false, null, null),

    /***************************************************
     *  ZOOKEEPER
     ***************************************************/

    // should not be used in URL because its coded into separate drivers
    ZOOKEEPER_DISCOVERY_ENABLED("zkEnabled", Boolean.FALSE.toString(), false, null, null),

    ZOOKEEPER_DISCOVERY_NAMESPACE("zkNamespace", "hiveserver2", false, null, null),
    ZOOKEEPER_DISCOVERY_RETRY("zkRetry", "1000", false, null, null),

    /***************************************************
     *  KERBEROS
     ***************************************************/

    KERBEROS_MODE("krb5Mode", KerberosMode.PASSWORD.name(), false, null, null, KerberosMode.KEYTAB.name(), KerberosMode.PASSWORD.name(), KerberosMode.PREAUTH.name()),
    // principal passed to thrift server using TSaslClientTransport.  this is not the local principal
    KERBEROS_SERVER_PRINCIPAL("krb5ServerPrincipal", null, false, null, null),
    // keytab used to authenticate when KerberosMode = KEYTAB; should be used in conjunction with USER property
    KERBEROS_USER_KEYTAB("krb5UserKeytab", null, false, null, null),
    // sun.security.krb5.debug
    KERBEROS_DEBUG_ENABLED("krb5Debug", Boolean.TRUE.toString(), false, null, null),
    // javax.security.auth.useSubjectCredsOnly; todo: i really don't get this property; see http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/BasicClientServer.html
    KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY("krb5SubjectOnly", Boolean.FALSE.toString(), false, null, null),

    // Sasl.QOP
    SASL_QUALITY_OF_PROTECTION("saslQOP", "auth-conf,auth-int,auth", false, null, null),
    // Sasl.SERVER_AUTH
    SASL_SERVER_AUTHENTICATION_ENABLED("saslAuth", Boolean.TRUE.toString(), false, null, null),

    JAAS_DEBUG_ENABLED("jaasDebug", Boolean.TRUE.toString(), false, null, null),

    ;


    private final String key;
    private final String defaultValue;
    private final boolean required;
    private final String description;
    private final String hiveConfigurationKey;
    private final String[] choices;


    HiveDriverProperty(String key, String defaultValue, boolean required, String description, String hiveConfigurationKey) {
        this(key, defaultValue, required, description, hiveConfigurationKey, (String[]) null);
    }

    HiveDriverProperty(String key, String defaultValue, boolean required, String description, String hiveConfigurationKey, String... choices) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.required = required;
        this.description = description;
        this.hiveConfigurationKey = hiveConfigurationKey;
        this.choices = choices;
    }

    public static HiveDriverProperty forAlias(String alias) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            if (property.hiveConfigurationKey != null && property.hiveConfigurationKey.equalsIgnoreCase(alias)) {
                return property;
            }
        }

        return null;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Properties properties) {
        if (defaultValue != null) {
            properties.setProperty(key, defaultValue);
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

    public String getHiveConfigurationKey() {
        return hiveConfigurationKey;
    }

    public void set(Properties properties, String value) {
        if (value == null) {
            properties.remove(key);
        } else {
            properties.setProperty(key, value);
        }
    }

    public void set(Properties properties, boolean value) {
        properties.setProperty(key, Boolean.toString(value));
    }

    public void set(Properties properties, int value) {
        properties.setProperty(key, Integer.toString(value));
    }

    public String get(Properties properties) {
        return properties.getProperty(key, defaultValue);
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
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, get(properties));
        propertyInfo.required = required;
        propertyInfo.description = description;
        propertyInfo.choices = choices;

        return propertyInfo;
    }


    @Override
    public String toString() {
        return this.key;
    }
}
