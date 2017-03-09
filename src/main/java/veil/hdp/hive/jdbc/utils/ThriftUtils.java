package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.ConnectionParameters;

import javax.security.sasl.SaslException;


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

    public static TTransport createBinaryTransport(ConnectionParameters connectionParameters, int loginTimeoutMilliseconds) throws SaslException {

        if (connectionParameters.isNoSasl()) {
            return HiveAuthFactory.getSocketTransport(connectionParameters.getHost(), connectionParameters.getPort(), loginTimeoutMilliseconds);
        } else {
            //no support for delegation tokens or ssl yet

            TTransport socketTransport = HiveAuthFactory.getSocketTransport(connectionParameters.getHost(), connectionParameters.getPort(), loginTimeoutMilliseconds);

            //hack: password can't be empty.  must always specify a non-null, non-empty string
            return PlainSaslHelper.getPlainTransport(connectionParameters.getUser(), connectionParameters.getPassword(), socketTransport);

        }

    }


}
