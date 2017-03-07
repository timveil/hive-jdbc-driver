package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;
import veil.hdp.hive.jdbc.utils.PropertyUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;
import veil.hdp.hive.jdbc.utils.UrlUtils;

import javax.security.sasl.SaslException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HiveConnection extends AbstractConnection {

    private static final Logger log = LoggerFactory.getLogger(HiveConnection.class);

    private TTransport transport;
    private TCLIService.Client thriftClient;
    private TSessionHandle sessionHandle;

    private boolean sessionClosed = true;


    public HiveConnection(String url, Properties info) throws TException, SaslException {

        ConnectionParameters connectionParameters = UrlUtils.parseURL(url);

        PropertyUtils.mergeProperties(connectionParameters, info);

        log.debug(connectionParameters.toString());

        if (!connectionParameters.isEmbeddedMode()) {

            transport = ThriftUtils.createBinaryTransport(connectionParameters, getLoginTimeout());

            ThriftUtils.openTransport(transport);

            thriftClient = new TCLIService.Client(new TBinaryProtocol(transport));

            sessionHandle = HiveServiceUtils.openSession(connectionParameters, thriftClient);

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

            HiveServiceUtils.closeSession(thriftClient, sessionHandle);

            ThriftUtils.closeTransport(transport);

            this.sessionClosed = true;
        }

    }

    @Override
    public boolean isClosed() throws SQLException {
        return sessionClosed;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new HiveStatement(this, thriftClient, sessionHandle);
    }
}
