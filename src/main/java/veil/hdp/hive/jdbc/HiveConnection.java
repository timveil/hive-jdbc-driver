package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.ThriftUtils;
import veil.hdp.hive.jdbc.utils.UrlUtils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HiveConnection extends BaseHiveConnection {

    private static final Logger log = LoggerFactory.getLogger(HiveConnection.class);

    private TTransport transport;
    private TCLIService.Client thriftClient;
    private TSessionHandle sessionHandle;

    private boolean sessionClosed = true;


    public HiveConnection(String url, Properties info) throws SQLException {

        ConnectionParameters connectionParameters = UrlUtils.parseURL(url);

        if (!connectionParameters.isEmbeddedMode()) {

            transport = ThriftUtils.createBinaryTransport(connectionParameters, getLoginTimeout());

            thriftClient = new TCLIService.Client(new TBinaryProtocol(transport));

            sessionHandle = ThriftUtils.openSession(connectionParameters, thriftClient);

            sessionClosed = false;

            //syncronize client?
        }

    }



    private int getLoginTimeout() {
        long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());

        if (timeOut > Integer.MAX_VALUE) {
            timeOut = Integer.MAX_VALUE;
        }

        return (int) timeOut;
    }


    @Override
    public void close() throws SQLException {

        if (!isClosed()) {

            TCloseSessionReq closeRequest = new TCloseSessionReq(sessionHandle);

            try {
                thriftClient.CloseSession(closeRequest);
            } catch (TException e) {
                log.warn(e.getMessage(), e);
            }

            try {
                transport.close();
            } finally {
                this.sessionClosed = true;
            }
        }

    }

    @Override
    public boolean isClosed() throws SQLException {
        return sessionClosed;
    }

}
