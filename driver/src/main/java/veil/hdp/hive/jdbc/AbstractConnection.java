package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.HiveDriver;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


abstract class AbstractConnection implements Connection {

    @Override
    public final CallableStatement prepareCall(String sql) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final String nativeSQL(String sql) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void commit() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void rollback() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Map<String, Class<?>> getTypeMap() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Savepoint setSavepoint() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Savepoint setSavepoint(String name) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void rollback(Savepoint savepoint) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Clob createClob() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Blob createBlob() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final NClob createNClob() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final SQLXML createSQLXML() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public final String getClientInfo(String name) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Properties getClientInfo() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public final Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void close() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getCatalog() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getHoldability() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSchema() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
