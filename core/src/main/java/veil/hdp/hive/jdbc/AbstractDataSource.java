package veil.hdp.hive.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

class AbstractDataSource implements DataSource {
    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final PrintWriter getLogWriter() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void setLogWriter(PrintWriter out) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
