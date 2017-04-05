package veil.hdp.hive.jdbc.utils;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import veil.hdp.hive.jdbc.HiveDriverProperty;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;


public class BinaryUtils {


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
