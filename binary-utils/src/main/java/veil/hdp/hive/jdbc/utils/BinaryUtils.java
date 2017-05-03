package veil.hdp.hive.jdbc.utils;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.security.PlainCallbackHandler;

import javax.security.sasl.SaslException;
import java.sql.SQLException;
import java.util.Properties;


public class BinaryUtils {


    private static final String PLAIN = "PLAIN";


    public static TSocket createSocket(Properties properties, int loginTimeoutMilliseconds) throws SQLException {

        String host = HiveDriverProperty.HOST_NAME.get(properties);
        int port = HiveDriverProperty.PORT_NUMBER.getInt(properties);

        return new TSocket(host, port, loginTimeoutMilliseconds);

    }

    public static TTransport createBinaryTransport(Properties properties, int loginTimeoutMilliseconds) throws SQLException {
        // todo: no support for no-sasl
        // todo: no support for delegation tokens or ssl yet

        String user = HiveDriverProperty.USER.get(properties);
        String password = HiveDriverProperty.PASSWORD.get(properties);

        TSocket socket = createSocket(properties, loginTimeoutMilliseconds);

        try {
            return new TSaslClientTransport(PLAIN, null, null, null, null, new PlainCallbackHandler(user, password), socket);
        } catch (SaslException e) {
            throw new HiveSQLException(e);
        }

    }
}
