package veil.hdp.hive.jdbc;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.Utils;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName) {
        return new SQLFeatureNotSupportedException(Utils.format("Method {} is not yet implemented.", callClass.getName() + '.' + functionName));
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass) {
        return notImplemented(callClass, "unknown");
    }

    private Connection connect(Properties properties) throws SQLException {
        return new HiveConnection.Builder().properties(properties).transport(buildTransport(properties)).build();
    }

    abstract TTransport buildTransport(Properties properties) throws SQLException;

    abstract PropertiesCallback buildPropertiesCallback();

    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            return connect(DriverUtils.buildProperties(url, info, buildPropertiesCallback()));
        }

        return null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverUtils.buildDriverPropertyInfo(url, info, buildPropertiesCallback());
    }

    @Override
    public int getMajorVersion() {
        return Constants.DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return Constants.DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return Boolean.FALSE;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw HiveDriver.notImplemented(this.getClass(), "getParentLogger");
    }

    public boolean acceptsURL(String url) throws SQLException {

        if (url == null) {
            throw new SQLException("url is null");
        }

        return DriverUtils.acceptURL(url);
    }

    static int getLoginTimeout() {
        long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());

        if (timeOut > Integer.MAX_VALUE) {
            timeOut = Integer.MAX_VALUE;
        }

        return (int) timeOut;
    }


}
