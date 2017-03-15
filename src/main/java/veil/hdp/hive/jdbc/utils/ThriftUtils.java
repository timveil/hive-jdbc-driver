package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverIntProperty;
import veil.hdp.hive.jdbc.HiveDriverStringProperty;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import java.util.HashMap;
import java.util.Properties;


public class ThriftUtils {

    private static final Logger log = LoggerFactory.getLogger(ThriftUtils.class);

    public static void openTransport(TTransport transport) throws TTransportException {

        if (!transport.isOpen()) {
            transport.open();
        }

    }

    public static void closeTransport(TTransport transport) {

        if (transport.isOpen()) {
            transport.close();
        }

    }

    public static TCLIService.Client createClient(TTransport transport) {
        return new TCLIService.Client(new TBinaryProtocol(transport));
    }


    public static TTransport createHttpTransport(Properties properties, CloseableHttpClient client) throws TTransportException {
        String host = properties.getProperty(HiveDriverStringProperty.HOST.getName());
        int port = Integer.parseInt(properties.getProperty(HiveDriverIntProperty.PORT_NUMBER.getName()));

        //todo still hardcoding http path and scheme
        return new THttpClient("http://" + host + ":" + port + "/cliservice", client);

    }

    public static TTransport createBinaryTransport(Properties properties, int loginTimeoutMilliseconds) throws SaslException {
        // todo: no support for no-sasl
        // todo: no support for delegation tokens or ssl yet

        String user = properties.getProperty(HiveDriverStringProperty.USER.getName());
        String password = properties.getProperty(HiveDriverStringProperty.PASSWORD.getName());
        String host = properties.getProperty(HiveDriverStringProperty.HOST.getName());
        int port = Integer.parseInt(properties.getProperty(HiveDriverIntProperty.PORT_NUMBER.getName()));

        TTransport socketTransport = new TSocket(host, port, loginTimeoutMilliseconds);

        return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<>(),
                callbacks -> {
                    for (Callback callback : callbacks) {
                        if (callback instanceof NameCallback) {
                            NameCallback nameCallback = (NameCallback) callback;
                            nameCallback.setName(user);
                        } else if (callback instanceof PasswordCallback) {
                            PasswordCallback passCallback = (PasswordCallback) callback;

                            if (password != null) {
                                passCallback.setPassword(password.toCharArray());
                            }
                        } else {
                            throw new UnsupportedCallbackException(callback);
                        }
                    }
                }, socketTransport);

    }

}
