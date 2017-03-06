package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.UrlUtils;

import java.sql.*;
import java.util.Properties;

public class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? new HiveConnection(url, info) : null;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return UrlUtils.acceptURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging not supported");
    }
}
