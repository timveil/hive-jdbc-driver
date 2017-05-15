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
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
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

        try {
            // some userid must be specified.  it can really be anything when AuthenticationMode = NONE
            return new TSaslClientTransport(MECHANISM_PLAIN,
                    null,
                    null,
                    null,
                    null,
                    new AnonymousCallbackHandler(),
                    socket);
        } catch (SaslException e) {
            throw new HiveException(e);
        }
    }

    private static TTransport buildSocketWithKerberos(Properties properties, TSocket socket) {

        try {

            System.setProperty("sun.security.krb5.debug", HiveDriverProperty.KERBEROS_DEBUG_ENABLED.get(properties));
            System.setProperty("javax.security.auth.useSubjectCredsOnly", HiveDriverProperty.KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY.get(properties));

            ServicePrincipal serverPrincipal = PrincipalUtils.parseServicePrincipal(HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties));

            Map<String, String> saslProps = new HashMap<>(2);
            saslProps.put(Sasl.QOP, HiveDriverProperty.SASL_QUALITY_OF_PROTECTION.get(properties));
            saslProps.put(Sasl.SERVER_AUTH, HiveDriverProperty.SASL_SERVER_AUTHENTICATION_ENABLED.get(properties));

            TTransport saslTransport = new TSaslClientTransport(
                    MECHANISM_KERBEROS,
                    null,
                    serverPrincipal.getService(),
                    serverPrincipal.getHost(),
                    saslProps,
                    null,
                    socket);

            Subject subject = null;

            KerberosMode kerberosMode = KerberosMode.valueOf(HiveDriverProperty.KERBEROS_MODE.get(properties));

            log.debug("kerberos mode [{}]", kerberosMode);

            boolean debugJaas = HiveDriverProperty.JAAS_DEBUG_ENABLED.getBoolean(properties);

            switch (kerberosMode) {

                case KEYTAB:

                    String keyTab = HiveDriverProperty.KERBEROS_USER_KEYTAB.get(properties);
                    String keyTabPrincipal = HiveDriverProperty.USER.get(properties);

                    subject = KerberosService.loginWithKeytab(keyTab, keyTabPrincipal, debugJaas);

                    break;
                case PREAUTH:
                    subject = KerberosService.getPreAuthenticatedSubject();

                    break;
                case PASSWORD:

                    String principal = HiveDriverProperty.USER.get(properties);
                    String password = HiveDriverProperty.PASSWORD.get(properties);

                    subject = KerberosService.loginWithPassword(principal, password, debugJaas);

                    break;
            }

            if (subject == null) {
                throw new IllegalArgumentException("Subject is null.  This is likely an invalid configuration");
            }

            return new SecureTransport(saslTransport, subject);

        } catch (SaslException e) {
            throw new HiveException(e);
        } catch (LoginException e) {
            throw new HiveException(e);
        }

    }



}
