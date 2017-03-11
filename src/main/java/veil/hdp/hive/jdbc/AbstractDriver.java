package veil.hdp.hive.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class AbstractDriver implements Driver {
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
