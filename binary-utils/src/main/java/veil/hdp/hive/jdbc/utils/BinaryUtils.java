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
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


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
        // todo: no support for no-sasl
        // todo: no support for delegation tokens or ssl yet


        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        TSocket socket = createSocket(properties, loginTimeoutMilliseconds);

        try {
            switch (authenticationMode) {

                case NONE:

                    String user = HiveDriverProperty.USER.get(properties);
                    String password = HiveDriverProperty.PASSWORD.get(properties);

                    return buildSocketWithSASL(user, password, socket);
                case NOSASL:
                    return socket;
                case LDAP:
                    break;
                case KERBEROS:
                    return buildSocketWithKerberos(properties, socket);
                case PAM:
                    break;
            }
        } catch (HiveException e) {
            throw new HiveSQLException(e);
        }

        throw new HiveSQLException("Authentication Mode [" + authenticationMode + "] is not supported!");

    }

    private static TTransport buildSocketWithSASL(String user, String password, TSocket socket) {
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

            KerberosPrincipal serverPrincipal = PrincipalUtils.parsePrincipal(HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties));

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

            // todo: i can't find a difference in behavior between these two option with the original driver.  they both seem accomplish the same thing
            if (HiveDriverProperty.KERBEROS_PRE_AUTHENTICATION_ENABLED.getBoolean(properties)) {
                return new PreAuthenticatedSecureTransport(saslTransport);
            } else {

                Subject subject = new Subject();

                // todo: it seems i can comment this out and i'm still logged in suggesting that JAAS does not matter.
                // this is part of UGI but i don't see why it matters in JDBC
                LoginContext login = new LoginContext(JdbcConfiguration.SIMPLE_CONFIG_NAME, subject, null, new JdbcConfiguration(properties));
                login.login();

                return new SecureTransport(saslTransport, subject);
            }


        } catch (IOException | LoginException e) {
            throw new HiveException(e);
        }

    }
}
