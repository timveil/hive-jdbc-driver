package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.HiveConfiguration;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import javax.security.sasl.SaslException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    static {
        try {
            java.sql.DriverManager.registerDriver(new HiveDriver());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }


    public Connection connect(Properties properties) throws SQLException {
        Connection connection = null;


        try {

            //TTransport transport = ThriftUtils.createBinaryTransport(properties, getLoginTimeout());
            TTransport transport = ThriftUtils.createHttpTransport(properties);

            ThriftUtils.openTransport(transport);

            TCLIService.Client thriftClient = ThriftUtils.createClient(transport);

            TOpenSessionResp tOpenSessionResp = HiveServiceUtils.openSession(properties, thriftClient);
            Map<String, String> configuration = tOpenSessionResp.getConfiguration();

            if (log.isDebugEnabled()) {
                log.debug("configuration for session returned by thrift {}", configuration);
            }

            TProtocolVersion protocolVersion = tOpenSessionResp.getServerProtocolVersion();

            TSessionHandle sessionHandle = tOpenSessionResp.getSessionHandle();

            connection = new HiveConnection(properties, transport, thriftClient, sessionHandle, protocolVersion);

            // todo: implement connect method so much of this can move there.  shouldn't be in driver.

        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }


        return connection;
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            return connect(DriverUtils.buildProperties(url, info));
        }

        return null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverUtils.buildDriverPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean acceptsURL(String url) throws SQLException {

        if (url == null) {
            throw new SQLException("url is null");
        }

        return DriverUtils.acceptURL(url);
    }

    private int getLoginTimeout() {
        long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());

        if (timeOut > Integer.MAX_VALUE) {
            timeOut = Integer.MAX_VALUE;
        }

        return (int) timeOut;
    }


}
