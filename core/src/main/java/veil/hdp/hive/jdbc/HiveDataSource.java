package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.AbstractDataSource;
import veil.hdp.hive.jdbc.core.AuthenticationMode;
import veil.hdp.hive.jdbc.core.HiveDriverProperty;
import veil.hdp.hive.jdbc.core.TransportMode;
import veil.hdp.hive.jdbc.core.security.KerberosMode;
import veil.hdp.hive.jdbc.core.utils.DriverUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class HiveDataSource extends AbstractDataSource {

    private static final Logger log = LoggerFactory.getLogger(HiveDataSource.class);

    abstract HiveDriver builDriver();

    private String host;
    private String database;
    private String user;
    private String password;
    private Integer port;
    private TransportMode transportMode;
    private AuthenticationMode authMode;
    private Integer thriftVersion;
    private Integer thriftSocketTimeout;
    private Integer thriftConnectionTimeout;
    private Boolean httpsEnabled;
    private String httpEndpoint;
    private Boolean httpPoolEnabled;
    private Integer httpPoolMax;
    private Integer httpPoolMaxRoute;
    private Boolean httpCookieReplayEnabled;
    private String httpCookieName;
    private Boolean zkEnabled;
    private String zkNamespace;
    private Integer zkRetry;
    private KerberosMode krb5Mode;
    private String krb5ServerPrincipal;
    private String krb5UserKeytab;
    private Boolean krb5Debug;
    private Boolean krb5SubjectOnly;
    private String saslQOP;
    private Boolean saslAuth;
    private Boolean jaasDebug;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public TransportMode getTransportMode() {
        return transportMode;
    }

    public void setTransportMode(TransportMode transportMode) {
        this.transportMode = transportMode;
    }

    public AuthenticationMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(AuthenticationMode authMode) {
        this.authMode = authMode;
    }

    public Integer getThriftVersion() {
        return thriftVersion;
    }

    public void setThriftVersion(Integer thriftVersion) {
        this.thriftVersion = thriftVersion;
    }

    public Integer getThriftSocketTimeout() {
        return thriftSocketTimeout;
    }

    public void setThriftSocketTimeout(Integer thriftSocketTimeout) {
        this.thriftSocketTimeout = thriftSocketTimeout;
    }

    public Integer getThriftConnectionTimeout() {
        return thriftConnectionTimeout;
    }

    public void setThriftConnectionTimeout(Integer thriftConnectionTimeout) {
        this.thriftConnectionTimeout = thriftConnectionTimeout;
    }

    public Boolean getHttpsEnabled() {
        return httpsEnabled;
    }

    public void setHttpsEnabled(Boolean httpsEnabled) {
        this.httpsEnabled = httpsEnabled;
    }

    public String getHttpEndpoint() {
        return httpEndpoint;
    }

    public void setHttpEndpoint(String httpEndpoint) {
        this.httpEndpoint = httpEndpoint;
    }

    public Boolean getHttpPoolEnabled() {
        return httpPoolEnabled;
    }

    public void setHttpPoolEnabled(Boolean httpPoolEnabled) {
        this.httpPoolEnabled = httpPoolEnabled;
    }

    public Integer getHttpPoolMax() {
        return httpPoolMax;
    }

    public void setHttpPoolMax(Integer httpPoolMax) {
        this.httpPoolMax = httpPoolMax;
    }

    public Integer getHttpPoolMaxRoute() {
        return httpPoolMaxRoute;
    }

    public void setHttpPoolMaxRoute(Integer httpPoolMaxRoute) {
        this.httpPoolMaxRoute = httpPoolMaxRoute;
    }

    public Boolean getHttpCookieReplayEnabled() {
        return httpCookieReplayEnabled;
    }

    public void setHttpCookieReplayEnabled(Boolean httpCookieReplayEnabled) {
        this.httpCookieReplayEnabled = httpCookieReplayEnabled;
    }

    public String getHttpCookieName() {
        return httpCookieName;
    }

    public void setHttpCookieName(String httpCookieName) {
        this.httpCookieName = httpCookieName;
    }

    public Boolean getZkEnabled() {
        return zkEnabled;
    }

    public void setZkEnabled(Boolean zkEnabled) {
        this.zkEnabled = zkEnabled;
    }

    public String getZkNamespace() {
        return zkNamespace;
    }

    public void setZkNamespace(String zkNamespace) {
        this.zkNamespace = zkNamespace;
    }

    public Integer getZkRetry() {
        return zkRetry;
    }

    public void setZkRetry(Integer zkRetry) {
        this.zkRetry = zkRetry;
    }

    public KerberosMode getKrb5Mode() {
        return krb5Mode;
    }

    public void setKrb5Mode(KerberosMode krb5Mode) {
        this.krb5Mode = krb5Mode;
    }

    public String getKrb5ServerPrincipal() {
        return krb5ServerPrincipal;
    }

    public void setKrb5ServerPrincipal(String krb5ServerPrincipal) {
        this.krb5ServerPrincipal = krb5ServerPrincipal;
    }

    public String getKrb5UserKeytab() {
        return krb5UserKeytab;
    }

    public void setKrb5UserKeytab(String krb5UserKeytab) {
        this.krb5UserKeytab = krb5UserKeytab;
    }

    public Boolean getKrb5Debug() {
        return krb5Debug;
    }

    public void setKrb5Debug(Boolean krb5Debug) {
        this.krb5Debug = krb5Debug;
    }

    public Boolean getKrb5SubjectOnly() {
        return krb5SubjectOnly;
    }

    public void setKrb5SubjectOnly(Boolean krb5SubjectOnly) {
        this.krb5SubjectOnly = krb5SubjectOnly;
    }

    public String getSaslQOP() {
        return saslQOP;
    }

    public void setSaslQOP(String saslQOP) {
        this.saslQOP = saslQOP;
    }

    public Boolean getSaslAuth() {
        return saslAuth;
    }

    public void setSaslAuth(Boolean saslAuth) {
        this.saslAuth = saslAuth;
    }

    public Boolean getJaasDebug() {
        return jaasDebug;
    }

    public void setJaasDebug(Boolean jaasDebug) {
        this.jaasDebug = jaasDebug;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {

        HiveDriver driver = builDriver();

        Properties properties = buildProperties();

        String url = DriverUtils.buildUrl(properties);

        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {

        HiveDriver driver = builDriver();

        Properties properties = buildProperties();

        HiveDriverProperty.USER.set(properties, username);
        HiveDriverProperty.PASSWORD.set(properties, password);

        String url = DriverUtils.buildUrl(properties);

        return driver.connect(url, properties);
    }

    private Properties buildProperties() {
        Properties properties = new Properties();

        HiveDriverProperty.HOST_NAME.set(properties, host);
        HiveDriverProperty.DATABASE_NAME.set(properties, database);
        HiveDriverProperty.USER.set(properties, user);
        HiveDriverProperty.PASSWORD.set(properties, password);
        HiveDriverProperty.PORT_NUMBER.set(properties, port);

        if (transportMode != null) {
            HiveDriverProperty.TRANSPORT_MODE.set(properties, transportMode.name());
        }

        if (authMode != null) {
            HiveDriverProperty.AUTHENTICATION_MODE.set(properties, authMode.name());
        }

        HiveDriverProperty.THRIFT_PROTOCOL_VERSION.set(properties, thriftVersion);
        HiveDriverProperty.THRIFT_SOCKET_TIMEOUT.set(properties, thriftSocketTimeout);
        HiveDriverProperty.THRIFT_CONNECTION_TIMEOUT.set(properties, thriftConnectionTimeout);
        HiveDriverProperty.HTTP_SSL_ENABLED.set(properties, httpsEnabled);
        HiveDriverProperty.HTTP_ENDPOINT.set(properties, httpEndpoint);
        HiveDriverProperty.HTTP_POOL_ENABLED.set(properties, httpPoolEnabled);
        HiveDriverProperty.HTTP_POOL_MAX_TOTAL.set(properties, httpPoolMax);
        HiveDriverProperty.HTTP_POOL_MAX_TOTAL.set(properties, httpPoolMax);
        HiveDriverProperty.HTTP_COOKIE_REPLAY_ENABLED.set(properties, httpCookieReplayEnabled);
        HiveDriverProperty.HTTP_COOKIE_NAME.set(properties, httpCookieName);
        HiveDriverProperty.ZOOKEEPER_DISCOVERY_ENABLED.set(properties, zkEnabled);
        HiveDriverProperty.ZOOKEEPER_DISCOVERY_NAMESPACE.set(properties, zkNamespace);
        HiveDriverProperty.ZOOKEEPER_DISCOVERY_RETRY.set(properties, zkRetry);

        if (krb5Mode != null) {
            HiveDriverProperty.KERBEROS_MODE.set(properties, krb5Mode.name());
        }

        HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.set(properties, krb5ServerPrincipal);
        HiveDriverProperty.KERBEROS_USER_KEYTAB.set(properties, krb5UserKeytab);
        HiveDriverProperty.KERBEROS_DEBUG_ENABLED.set(properties, krb5Debug);
        HiveDriverProperty.KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY.set(properties, krb5SubjectOnly);
        HiveDriverProperty.SASL_QUALITY_OF_PROTECTION.set(properties, saslQOP);
        HiveDriverProperty.SASL_SERVER_AUTHENTICATION_ENABLED.set(properties, saslAuth);
        HiveDriverProperty.JAAS_DEBUG_ENABLED.set(properties, jaasDebug);

        return properties;

    }




}
