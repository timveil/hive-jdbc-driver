package veil.hdp.hive.jdbc.binary;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.AuthenticationMode;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.core.security.*;

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

    public static TSocket createSocket(Properties properties) {

        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        int socketTimeout = HiveDriverProperty.THRIFT_SOCKET_TIMEOUT.getInt(properties);
        int connectionTimeout = HiveDriverProperty.THRIFT_CONNECTION_TIMEOUT.getInt(properties);

        return new TSocket(host, port, socketTimeout, connectionTimeout);

    }

    public static TTransport createBinaryTransport(Properties properties) throws SQLException {
        // todo: no support for delegation tokens or ssl yet


        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        TSocket socket = createSocket(properties);

        try {
            switch (authenticationMode) {

                case NONE:
                    return buildSocketWithSASL(properties, socket);
                case NOSASL:
                    return socket;
                case KERBEROS:
                    return buildSocketWithKerberos(properties, socket);

            }
        } catch (HiveException e) {
            throw new HiveSQLException(e);
        }

        throw new HiveSQLException("Authentication Mode [" + authenticationMode + "] is not supported!");

    }

    private static TTransport buildSocketWithSASL(Properties properties, TSocket socket) {

        try {
            // some userid must be specified.  it can really be anything when AuthenticationMode = NONE
            return new TSaslClientTransport(SaslMechanism.PLAIN.name(),
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

            TTransport saslTransport = buildSaslTransport(properties, socket);

            Subject subject = KerberosService.getSubject(properties);

            if (subject == null) {
                throw new HiveException("Subject is null.  This is likely an invalid configuration");
            }


            return new JaasTransport(saslTransport, subject);

        } catch (SaslException | LoginException e) {
            throw new HiveException(e);
        }

    }

    private static TTransport buildSaslTransport(Properties properties, TSocket socket) throws SaslException {
        ServicePrincipal serverPrincipal = PrincipalUtils.parseServicePrincipal(HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties));

        Map<String, String> saslProps = new HashMap<>(2);
        saslProps.put(Sasl.QOP, HiveDriverProperty.SASL_QUALITY_OF_PROTECTION.get(properties));
        saslProps.put(Sasl.SERVER_AUTH, HiveDriverProperty.SASL_SERVER_AUTHENTICATION_ENABLED.get(properties));

        return new TSaslClientTransport(
                SaslMechanism.GSSAPI.name(),
                null,
                serverPrincipal.getService(),
                serverPrincipal.getHost(),
                saslProps,
                null,
                socket);
    }




}
