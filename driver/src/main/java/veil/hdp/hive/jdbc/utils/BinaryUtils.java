package veil.hdp.hive.jdbc.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import veil.hdp.hive.jdbc.AuthenticationMode;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.security.*;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public final class BinaryUtils {

    private static final Logger log = LogManager.getLogger(BinaryUtils.class);
    private static final String ENDPOINT_IDENTIFICATION_ALGORITHM_NAME = "HTTPS";

    private BinaryUtils() {
    }

    private static TSocket createSocket(Properties properties) {

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

    private static TSocket buildSSLSocket(Properties properties, String host, int port, int socketTimeout) {
        try {

            TSocket socket;

            if (HiveDriverProperty.SSL_TRUST_STORE_PATH.hasValue(properties)) {
                TSSLTransportParameters params = new TSSLTransportParameters();
                params.setTrustStore(HiveDriverProperty.SSL_TRUST_STORE_PATH.get(properties), HiveDriverProperty.SSL_TRUST_STORE_PASSWORD.get(properties), null, HiveDriverProperty.SSL_TRUST_STORE_TYPE.get(properties));
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
            throw new HiveException(e);
        }
    }

    public static TTransport createBinaryTransport(Properties properties) {
        // todo: no support for delegation tokens


        AuthenticationMode authenticationMode = AuthenticationMode.valueOf(HiveDriverProperty.AUTHENTICATION_MODE.get(properties));

        TSocket socket = createSocket(properties);

        switch (authenticationMode) {

            case NONE:
                return buildSocketWithSASL(socket);
            case NOSASL:
                return socket;
            case KERBEROS:
                return buildSocketWithKerberos(properties, socket);

        }


        throw new HiveException("Authentication Mode [" + authenticationMode + "] is not supported when creating a Binary Transport!");

    }

    private static TTransport buildSocketWithSASL(TSocket socket) {

        try {

            SaslClient saslClient = Sasl.createSaslClient(new String[]{SaslMechanism.PLAIN.name()}, null, null, null, null, new AnonymousCallbackHandler());

            // some userid must be specified.  it can really be anything when AuthenticationMode = NONE
            return new TSaslClientTransport(saslClient, socket);

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

        ServicePrincipal servicePrincipal = PrincipalUtils.parseServicePrincipal(HiveDriverProperty.KERBEROS_SERVER_PRINCIPAL.get(properties), HiveDriverProperty.HOST_NAME.get(properties));

        log.debug("service principal [{}]", servicePrincipal);

        Map<String, String> saslProps = new HashMap<>(2);
        saslProps.put(Sasl.QOP, HiveDriverProperty.SASL_QUALITY_OF_PROTECTION.get(properties));
        saslProps.put(Sasl.SERVER_AUTH, HiveDriverProperty.SASL_SERVER_AUTHENTICATION_ENABLED.get(properties));

        SaslClient saslClient = Sasl.createSaslClient(new String[]{SaslMechanism.GSSAPI.name()}, null, servicePrincipal.getService(), servicePrincipal.getHost(), saslProps, new TextCallbackHandler());

        return new TSaslClientTransport(saslClient, socket);
    }


}
