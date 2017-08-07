package veil.hdp.hive.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.*;
import veil.hdp.hive.jdbc.core.thrift.ThriftTransport;
import veil.hdp.hive.jdbc.core.utils.DriverUtils;
import veil.hdp.hive.jdbc.core.utils.PropertyUtils;
import veil.hdp.hive.jdbc.core.utils.VersionUtils;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;

public abstract class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);
    private static final String NOT_PROVIDED = "NOT PROVIDED";
    private static final String UNKNOWN = "UNKNOWN";

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName, String reason) {
        return new SQLFeatureNotSupportedException(MessageFormat.format("Method [{0}] is not implemented. Reason: [{1}]", callClass.getName() + '.' + functionName, reason));
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName) {
        return notImplemented(callClass, functionName, NOT_PROVIDED);
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass) {
        return notImplemented(callClass, UNKNOWN);
    }

    private Connection connect(Properties properties) throws SQLException {
        ThriftTransport thriftTransport = buildTransport(properties);

        try {
            return HiveConnection.builder().properties(properties).thriftTransport(thriftTransport).build();
        } catch (HiveException e) {
            try {
                thriftTransport.close();
            } catch (IOException ignore) {
            }

            throw new HiveSQLException("There was a problem building the HiveConnection.", e);
        }
    }

    abstract ThriftTransport buildTransport(Properties properties) throws SQLException;

    abstract DefaultDriverProperties defaultDriverProperties();

    abstract UriProperties uriProperties();

    abstract ZookeeperDiscoveryProperties zookeeperDiscoveryProperties();

    public Connection connect(String url, Properties info) throws SQLException {


        log.info("connecting to hive version [{}] with thrift protocol version [{}].  Please ensure these match your HiveServer2 instance or unexpected errors and behavior may occur.",
                PropertyUtils.getInstance().getValue("hive.version"),
                PropertyUtils.getInstance().getValue("thrift.protocol.version.default"));

        url = StringUtils.trimToNull(url);

        if (acceptsURL(url)) {
            return connect(DriverUtils.buildProperties(url, info, defaultDriverProperties(), uriProperties(), zookeeperDiscoveryProperties()));
        }

        return null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverUtils.buildDriverPropertyInfo(url, info, defaultDriverProperties(), uriProperties(), zookeeperDiscoveryProperties());
    }

    @Override
    public int getMajorVersion() {
        return VersionUtils.getDriverMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return VersionUtils.getDriverMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return Boolean.FALSE;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw HiveDriver.notImplemented(this.getClass(), "getParentLogger", "This driver uses SLF4J instead of JUL directly for performance reasons");
    }

    public boolean acceptsURL(String url) throws SQLException {
        return DriverUtils.acceptURL(url);
    }


}
