package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;
import veil.hdp.hive.jdbc.utils.PropertyUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;
import veil.hdp.hive.jdbc.utils.UrlUtils;

import javax.security.sasl.SaslException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HiveDriver extends AbstractHiveDriver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    static {
        try {
            java.sql.DriverManager.registerDriver(new HiveDriver());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {

        Connection connection = null;

        if (acceptsURL(url)) {

            try {

                ConnectionParameters connectionParameters = UrlUtils.parseURL(url);

                PropertyUtils.mergeProperties(connectionParameters, info);

                if (log.isDebugEnabled()) {
                    log.debug(connectionParameters.toString());
                }

                TTransport transport = ThriftUtils.createBinaryTransport(connectionParameters, getLoginTimeout());

                ThriftUtils.openTransport(transport);

                TCLIService.Client thriftClient = ThriftUtils.createClient(transport);

                TOpenSessionResp tOpenSessionResp = HiveServiceUtils.openSession(connectionParameters, thriftClient);

                TProtocolVersion protocolVersion = tOpenSessionResp.getServerProtocolVersion();

                TSessionHandle sessionHandle = tOpenSessionResp.getSessionHandle();

                connection = new HiveConnection(connectionParameters, transport, thriftClient, sessionHandle, protocolVersion);

            } catch (SaslException | TException e) {
                throw new SQLException(e.getMessage(), "", e);
            }
        }

        return connection;

    }

    public boolean acceptsURL(String url) throws SQLException {

        if (url == null) {
            throw new SQLException("url is null");
        }

        return UrlUtils.acceptURL(url);
    }

    private int getLoginTimeout() {
        long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());

        if (timeOut > Integer.MAX_VALUE) {
            timeOut = Integer.MAX_VALUE;
        }

        return (int) timeOut;
    }


}
