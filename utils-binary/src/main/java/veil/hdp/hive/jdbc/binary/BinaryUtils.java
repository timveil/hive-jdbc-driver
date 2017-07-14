package veil.hdp.hive.jdbc.binary;

import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.AuthenticationMode;
import veil.hdp.hive.jdbc.core.HiveDriverProperty;
import veil.hdp.hive.jdbc.core.HiveException;
import veil.hdp.hive.jdbc.core.HiveSQLException;
import veil.hdp.hive.jdbc.core.security.*;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
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
    private static final String ENDPOINT_IDENTIFICATION_ALGORITHM_NAME = "HTTPS";

    public static TSocket createSocket(Properties properties) throws HiveSQLException {

        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        int socketTimeout = HiveDriverProperty.THRIFT_SOCKET_TIMEOUT.getInt(properties);
        int connectionTimeout = HiveDriverProperty.THRIFT_CONNECTION_TIMEOUT.getInt(properties);

        if (HiveDriverProperty.SSL_ENABLED.getBoolean(properties)) {

            return buildSSLSocket(properties, host, port, socketTimeout);

        } else {

            return new TSocket(host, port, socketTimeout, connectionTimeout);
        }

    }

    private static TSocket buildSSLSocket(Properties properties, String host, int port, int socketTimeout) throws HiveSQLException {
        try {

            TSocket socket;

            if (HiveDriverProperty.SSL_TRUST_STORE_PATH.hasValue(properties)) {
                TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
                params.setTrustStore(HiveDriverProperty.SSL_TRUST_STORE_PATH.get(properties), HiveDriverProperty.SSL_TRUST_STORE_PASSWORD.get(properties));
                params.requireClientAuth(true);

                socket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout, params);
            } else {

                socket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout);
            }

            SSLSocket sslSocket = (SSLSocket) socket.getSocket();
            SSLParameters sslParams = sslSocket.getSSLParameters();
            sslParams.setEndpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION_ALGORITHM_NAME);
            sslSocket.setSSLParameters(sslParams);

            return new TSocket(sslSocket);

        } catch (TTransportException e) {
            throw new HiveSQLException(e);
        }
    }

    public static TTransport createBinaryTransport(Properties properties) throws SQLException {
        // todo: no support for delegation tokens


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
