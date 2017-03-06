package veil.hdp.hive.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


public class BaseHiveConnection implements Connection {
    @Override
    public Statement createStatement() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void close() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }
}
