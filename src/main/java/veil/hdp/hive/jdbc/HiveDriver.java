package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.Utils;

import java.sql.*;
import java.util.Properties;

public class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    static {
        try {
            HiveDriver driver = new HiveDriver();

            DriverManager.registerDriver(driver);

            if (log.isInfoEnabled()) {
                log.info("driver [{}] has been registered.", driver.getClass().getName());
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName) {
        return new SQLFeatureNotSupportedException(Utils.format("Method {0} is not yet implemented.", callClass.getName() + "." + functionName));
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass) {
        return notImplemented(callClass, "unknown");
    }

    private Connection connect(Properties properties) throws SQLException {
        return new HiveConnection.Builder().properties(properties).build();
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
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
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


}
