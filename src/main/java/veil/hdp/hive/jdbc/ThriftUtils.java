package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;


public class ThriftUtils {

    private static final Logger log = LoggerFactory.getLogger(ThriftUtils.class);

    public static void openTransport(TTransport transport) throws SQLException {

        if (!transport.isOpen()) {
            try {
                transport.open();
            } catch (TTransportException e) {
                throw new HiveThriftException(e);
            }
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


    public static TTransport createHttpTransport(Properties properties, CloseableHttpClient client) throws SQLException {
        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        // todo: still hard-coding http path and scheme
        try {
            return new THttpClient("http://" + host + ":" + port + "/cliservice", client);
        } catch (TTransportException e) {
            throw new HiveThriftException(e);
        }

    }

    public static TTransport createBinaryTransport(Properties properties, int loginTimeoutMilliseconds) throws SQLException {
        // todo: no support for no-sasl
        // todo: no support for delegation tokens or ssl yet

        String user = HiveDriverProperty.USER.get(properties);
        String password = HiveDriverProperty.PASSWORD.get(properties);
        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        TTransport socketTransport = new TSocket(host, port, loginTimeoutMilliseconds);

        try {
            return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<>(),
                    callbacks -> {
                        for (Callback callback : callbacks) {
                            if (callback instanceof NameCallback) {
                                NameCallback nameCallback = (NameCallback) callback;
                                nameCallback.setName(user);
                            } else if (callback instanceof PasswordCallback) {
                                PasswordCallback passwordCallback = (PasswordCallback) callback;

                                if (password != null) {
                                    passwordCallback.setPassword(password.toCharArray());
                                } else {
                                    // todo:hack: for some reason this can't be null or empty string; set default value
                                    passwordCallback.setPassword("anonymous".toCharArray());
                                }

                            } else {
                                throw new UnsupportedCallbackException(callback);
                            }
                        }
                    }, socketTransport);
        } catch (SaslException e) {
            throw new SQLException(e);
        }

    }

}
