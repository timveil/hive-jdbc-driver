/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import org.apache.commons.lang3.StringUtils;
import veil.hdp.hive.jdbc.security.KerberosMode;
import veil.hdp.hive.jdbc.security.SaslQop;
import veil.hdp.hive.jdbc.utils.PropertyUtils;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.Properties;

public enum HiveDriverProperty {


    /***************************************************
     *  COMMON
     ***************************************************/
    // required by JDBC Spec
    HOST_NAME("host", null, null, "hive.server2.thrift.bind.host"),
    DATABASE_NAME("database", "default", null, null),

    // when AUTHENTICATION_MODE == NONE then this is ignored; can be principal when using KERBEROS_MODE = PASSWORD
    USER("user", null, null, null),

    // when AUTHENTICATION_MODE == NONE then this is ignored
    PASSWORD("password", null, null, null),

    PORT_NUMBER("port", "10000", null, "hive.server2.thrift.port"),

    TRANSPORT_MODE("transportMode", TransportMode.binary.name(), null, "hive.server2.transport.mode", new String[]{TransportMode.binary.name(), TransportMode.http.name()}, null),

    AUTHENTICATION_MODE("authMode", AuthenticationMode.NONE.name(), null, "hive.server2.authentication",
            new String[]{AuthenticationMode.NONE.name(),
                    AuthenticationMode.NOSASL.name(),
                    AuthenticationMode.KERBEROS.name(),
                    AuthenticationMode.LDAP.name(),
                    AuthenticationMode.PAM.name()}, null),

    // make sure to spell out differences in readme; look at *.thrift
    THRIFT_PROTOCOL_VERSION("thriftVersion", PropertyUtils.getInstance().getValue("thrift.protocol.version.default"), null, null),

    // in milliseconds. time code will wait to open thrift transport
    THRIFT_TRANSPORT_TIMEOUT("thriftTransportTimeout", "10000", null, null),

    FETCH_SIZE("fetchSize", "1000", null, null),

    FETCH_SERVER_LOGS("fetchLogs", Boolean.FALSE.toString(), null, null),


    /***************************************************
     *  BINARY
     ***************************************************/

    THRIFT_SOCKET_TIMEOUT("thriftSocketTimeout", "0", null, null),

    THRIFT_CONNECTION_TIMEOUT("thriftConnectionTimeout", "0", null, null),

    /***************************************************
     *  SSL
     ***************************************************/

    SSL_ENABLED("sslEnabled", Boolean.FALSE.toString(), null, "hive.server2.use.ssl", null, new String[]{"ssl"}),

    SSL_TRUST_STORE_PATH("sslTrustStorePath", null, null, null, null, new String[]{"sslTrustStore"}),
    SSL_TRUST_STORE_TYPE("sslTrustStoreType", "JKS", null, null),
    SSL_TRUST_STORE_PASSWORD("sslTrustStorePassword", null, null, null, null, new String[]{"trustStorePassword"}),

    SSL_TWO_WAY_ENABLED("sslTwoWayEnabled", Boolean.FALSE.toString(), null, null, null, new String[]{"twoWay"}),

    SSL_KEY_STORE_PATH("sslKeyStorePath", null, null, null, null, new String[]{"sslKeyStore"}),
    SSL_KEY_STORE_TYPE("sslKeyStoreType", "JKS", null, null),
    SSL_KEY_STORE_PASSWORD("sslKeyStorePassword", null, null, null, null, new String[]{"keyStorePassword"}),

    /***************************************************
     *  HTTP
     ***************************************************/

    HTTP_ENDPOINT("httpEndpoint", "cliservice", null, "hive.server2.thrift.http.path", null, new String[]{"httpPath"}),

    HTTP_POOL_ENABLED("httpPoolEnabled", Boolean.FALSE.toString(), null, null),
    HTTP_POOL_MAX_TOTAL("httpPoolMax", "100", null, null),
    HTTP_POOL_MAX_PER_ROUTE("httpPoolMaxRoute", "20", null, null),

    // https://issues.apache.org/jira/browse/HIVE-9709
    HTTP_COOKIE_REPLAY_ENABLED("httpCookieReplayEnabled", Boolean.TRUE.toString(), null, null, null, new String[]{"cookieAuth"}),

    // todo: this seems pretty hardcoded on the server side.  not sure how/why this would ever change
    HTTP_COOKIE_NAME("httpCookieName", "hive.server2.auth", null, null, null, new String[]{"cookieName"}),

    /***************************************************
     *  ZOOKEEPER
     ***************************************************/

    // should not be used in URL because its coded into separate drivers
    ZOOKEEPER_DISCOVERY_ENABLED("zkEnabled", Boolean.FALSE.toString(), null, null),

    ZOOKEEPER_DISCOVERY_NAMESPACE("zkNamespace", "hiveserver2", null, null, null, new String[]{"zooKeeperNamespace"}),
    ZOOKEEPER_DISCOVERY_RETRY("zkRetry", "1000", null, null),

    /***************************************************
     *  KERBEROS
     ***************************************************/

    KERBEROS_MODE("krb5Mode", KerberosMode.OS.name(), null, null, new String[]{KerberosMode.OS.name(), KerberosMode.KEYTAB.name(), KerberosMode.PASSWORD.name(), KerberosMode.PREAUTH.name()}, null),
    // principal passed to thrift server using TSaslClientTransport.  this is not the local principal
    KERBEROS_SERVER_PRINCIPAL("krb5ServerPrincipal", null, null, "hive.server2.authentication.kerberos.principal", null, new String[]{"principal"}),
    // keytab used to authenticate when KerberosMode = KEYTAB; should be used in conjunction with USER property
    KERBEROS_USER_KEYTAB("krb5UserKeytab", null, null, null),
    // sun.security.krb5.debug
    KERBEROS_DEBUG_ENABLED("krb5Debug", Boolean.FALSE.toString(), null, null),
    // javax.security.auth.useSubjectCredsOnly; todo: i really don't get this property; see http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/BasicClientServer.html
    KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY("krb5SubjectOnly", Boolean.FALSE.toString(), null, null),

    // Sasl.QOP
    SASL_QUALITY_OF_PROTECTION("saslQOP", SaslQop.AUTH.getValue(), null, "hive.server2.thrift.sasl.qop", new String[]{SaslQop.AUTH.getValue(), SaslQop.AUTH_INT.getValue(), SaslQop.AUTH_CONF.getValue()}, null),
    // Sasl.SERVER_AUTH
    SASL_SERVER_AUTHENTICATION_ENABLED("saslAuth", Boolean.TRUE.toString(), null, null),

    JAAS_DEBUG_ENABLED("jaasDebug", Boolean.FALSE.toString(), null, null),;


    private final String key;
    private final String defaultValue;
    private final String description;
    private final String hiveConfigurationKey;
    private final String[] choices;
    private final String[] aliases;


    HiveDriverProperty(String key, String defaultValue, String description, String hiveConfigurationKey) {
        this(key, defaultValue, description, hiveConfigurationKey, null, null);
    }

    HiveDriverProperty(String key, String defaultValue, String description, String hiveConfigurationKey, String[] choices, String[] aliases) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
        this.hiveConfigurationKey = hiveConfigurationKey;
        this.choices = choices;
        this.aliases = aliases;
    }

    public static HiveDriverProperty forAlias(String alias) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {
            if (property.hiveConfigurationKey != null && property.hiveConfigurationKey.equalsIgnoreCase(alias)) {
                return property;
            }
        }

        return null;
    }

    public static HiveDriverProperty forKeyIgnoreCase(String key) {
        for (HiveDriverProperty property : HiveDriverProperty.values()) {

            if (property.key.equalsIgnoreCase(key) || (property.getAliases() != null && Arrays.stream(property.getAliases()).anyMatch(s -> s.equalsIgnoreCase(key)))) {
                return property;
            }
        }

        return null;
    }

    public String getKey() {
        return key;
    }

    public void setDefaultValue(Properties properties) {
        if (defaultValue != null) {
            properties.setProperty(key, defaultValue);
        }
    }

    public void set(Properties properties, String value) {
        if (value == null) {
            properties.remove(key);
        } else {
            properties.setProperty(key, value);
        }
    }

    public void set(Properties properties, Boolean value) {
        set(properties, value != null ? Boolean.toString(value) : null);
    }

    public void set(Properties properties, Integer value) {
        set(properties, value != null ? Integer.toString(value) : null);
    }

    public String get(Properties properties) {
        return properties.getProperty(key, defaultValue);
    }

    public boolean hasValue(Properties properties) {
        return properties.containsKey(key) && StringUtils.isNotBlank(get(properties));
    }

    public boolean getBoolean(Properties properties) {
        return Boolean.valueOf(get(properties));
    }

    public int getInt(Properties properties) {
        String value = get(properties);

        return Integer.parseInt(value);
    }

    public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, get(properties));
        propertyInfo.required = false;
        propertyInfo.description = description;
        propertyInfo.choices = choices;

        return propertyInfo;
    }

    String[] getAliases() {
        return aliases;
    }

    String getHiveConfigurationKey() {
        return hiveConfigurationKey;
    }

    @Override
    public String toString() {
        return this.key;
    }
}
