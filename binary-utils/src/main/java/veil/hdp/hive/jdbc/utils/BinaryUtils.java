package veil.hdp.hive.jdbc.utils;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.AuthenticationMode;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.security.*;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;


public class BinaryUtils {

    private static final Logger log = LoggerFactory.getLogger(BinaryUtils.class);

    // TODO: MAKE THESE VALUES PART OF ENUM, MAYBE AUTHENTICATIONMODE
    private static final String MECHANISM_PLAIN = "PLAIN";
    private static final String MECHANISM_KERBEROS = "GSSAPI";


    public static TSocket createSocket(Properties properties, int loginTimeoutMilliseconds) {

        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        return new TSocket(host, port, loginTimeoutMilliseconds);

    }

    public static TTransport createBinaryTransport(Properties properties, int loginTimeoutMilliseconds) throws SQLException {
        // todo: no support for delegation tokens or ssl yet


        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        TSocket socket = createSocket(properties, loginTimeoutMilliseconds);

        try {
            switch (authenticationMode) {

                case NONE:
                    return buildSocketWithSASL(properties, socket);
                case NOSASL:
                    return socket;
                case KERBEROS:
                    return buildSocketWithKerberos(properties, socket);
                case LDAP:
                case PAM:
                    break;
            }
        } catch (HiveException e) {
            throw new HiveSQLException(e);
        }

        throw new HiveSQLException("Authentication Mode [" + authenticationMode + "] is not supported!");

    }

    private static TTransport buildSocketWithSASL(Properties properties, TSocket socket) {

        String user = HiveDriverProperty.USER.get(properties);
        String password = HiveDriverProperty.PASSWORD.get(properties);

        try {
            return new TSaslClientTransport(MECHANISM_PLAIN,
                    null,
                    null,
                    null,
                    null,
                    new PlainCallbackHandler(user, password),
                    socket);
        } catch (SaslException e) {
            throw new HiveException(e);
        }
    }

    private static TTransport buildSocketWithKerberos(Properties properties, TSocket socket) {

        try {

            System.setProperty("sun.security.krb5.debug", HiveDriverProperty.KERBEROS_DEBUG_ENABLED.get(properties));
            System.setProperty("javax.security.auth.useSubjectCredsOnly", HiveDriverProperty.KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY.get(properties));

            String host = HiveDriverProperty.HOST_NAME.get(properties);

            ProvidedPrincipal serverPrincipal = PrincipalUtils.parsePrincipal(HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties));

            Map<String, String> saslProps = new HashMap<>(2);
            saslProps.put(Sasl.QOP, HiveDriverProperty.SASL_QUALITY_OF_PROTECTION.get(properties));
            saslProps.put(Sasl.SERVER_AUTH, HiveDriverProperty.SASL_SERVER_AUTHENTICATION_ENABLED.get(properties));

            TTransport saslTransport = new TSaslClientTransport(
                    MECHANISM_KERBEROS,
                    null,
                    serverPrincipal.getUser(),
                    serverPrincipal.getServer(),
                    saslProps,
                    null,
                    socket);

            Subject subject;

            if (HiveDriverProperty.KERBEROS_PRE_AUTHENTICATION_ENABLED.getBoolean(properties)) {

                AccessControlContext context = AccessController.getContext();
                subject = Subject.getSubject(context);

                if (subject == null) {
                    throw new IllegalArgumentException("KERBEROS_PRE_AUTHENTICATION_ENABLED is set to true but subject is null.  This is likely an invalid configuration");
                }

                log.debug("pre auth subject [{}]", subject);

            } else {
                subject = jaasLogin(properties);
            }


            return new SecureTransport(saslTransport, subject);

        } catch (SaslException e) {
            throw new HiveException(e);
        }

    }

    // todo: move this method and related out of this class
    private static Subject jaasLogin(Properties properties) {

        LoginContext loginContext;

        try {

            if (HiveDriverProperty.KERBEROS_LOCAL_KEYTAB.get(properties) != null) {
                // this will use the local keytab and principal passed in the url
                loginContext = keytabContext(properties, PlatformUtils.getKrb5LoginModuleClassName());
            } else {
                // this requires that kinit be called outside the driver
                loginContext = localContext(properties, PlatformUtils.getOSLoginModuleClassName());
            }

            loginContext.login();

            Subject clientSubject = loginContext.getSubject();

            log.debug("logged in subject [{}]", clientSubject);

            return clientSubject;

        } catch (LoginException e) {
            throw new HiveException(e);
        }

    }


    private static LoginContext keytabContext(Properties properties, String loginModuleClassName) throws LoginException {

        String configName = "fromKeytab";

        Map<String, String> options = new HashMap<>(7);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));
        options.put(LoginModuleConstants.DO_NOT_PROMPT, "true");
        options.put(LoginModuleConstants.KEY_TAB, HiveDriverProperty.KERBEROS_LOCAL_KEYTAB.get(properties));
        options.put(LoginModuleConstants.PRINCIPAL, HiveDriverProperty.KERBEROS_LOCAL_PRINCIPAL.get(properties));
        options.put(LoginModuleConstants.USE_KEY_TAB, "true");
        options.put(LoginModuleConstants.STORE_KEY, "true");
        options.put(LoginModuleConstants.REFRESH_KRB_5_CONFIG, "true");

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, loginModuleClassName, REQUIRED, options);

        return new LoginContext(configName, null, null, config);
    }

    private static LoginContext localContext(Properties properties, String loginModuleClassName) throws LoginException {

        String configName = "fromLocal";

        Map<String, String> options = new HashMap<>(1);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));

        // available on windows, but not unix
        options.put(LoginModuleConstants.DEBUG_NATIVE, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, loginModuleClassName, REQUIRED, options);

        return new LoginContext(configName, null, null, config);
    }

    private static LoginContext cacheContext(Properties properties, String loginModuleClassName) throws LoginException {

        String configName = "fromTicketCache";

        Map<String, String> options = new HashMap<>(4);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));
        options.put(LoginModuleConstants.DO_NOT_PROMPT, "true");
        options.put(LoginModuleConstants.USE_TICKET_CACHE, "true");
        options.put(LoginModuleConstants.RENEW_TGT, "true");

        String ticketCache = System.getenv("KRB5CCNAME");

        if (ticketCache != null) {
            options.put(LoginModuleConstants.TICKET_CACHE, ticketCache);
        }

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, loginModuleClassName, OPTIONAL, options);

        return new LoginContext(configName, null, null, config);
    }

    private class LoginModuleConstants {
        public static final String DEBUG = "debug";
        public static final String DEBUG_NATIVE = "debugNative";
        public static final String DO_NOT_PROMPT = "doNotPrompt";
        public static final String KEY_TAB = "keyTab";
        public static final String PRINCIPAL = "principal";
        public static final String USE_KEY_TAB = "useKeyTab";
        public static final String STORE_KEY = "storeKey";
        public static final String REFRESH_KRB_5_CONFIG = "refreshKrb5Config";
        public static final String USE_TICKET_CACHE = "useTicketCache";
        public static final String RENEW_TGT = "renewTGT";
        public static final String TICKET_CACHE = "ticketCache";
    }
}
