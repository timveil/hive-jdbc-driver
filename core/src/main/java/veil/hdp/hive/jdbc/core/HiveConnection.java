package veil.hdp.hive.jdbc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.thrift.ThriftTransport;
import veil.hdp.hive.jdbc.core.utils.QueryUtils;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.Executor;

public class HiveConnection extends AbstractConnection {

    private static final Logger log = LoggerFactory.getLogger(HiveConnection.class);

    private static final SQLPermission SQL_PERMISSION_ABORT = new SQLPermission("callAbort");

    // constructor
    private final ThriftSession thriftSession;

    // public getter & setter
    private SQLWarning sqlWarning = null;

    private HiveConnection(ThriftSession thriftSession) {
        this.thriftSession = thriftSession;
    }

    public static HiveConnectionBuilder builder() {
        return new HiveConnectionBuilder();
    }

    public ThriftSession getThriftSession() {
        return thriftSession;
    }

    @Override
    public void close() throws SQLException {

        try {
            thriftSession.close();
        } catch (IOException e) {
            log.warn(e.getMessage(), e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return thriftSession.isClosed();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return HiveStatement.builder()
                .connection(this)
                .type(ResultSet.TYPE_FORWARD_ONLY)
                .concurrency(ResultSet.CONCUR_READ_ONLY)
                .holdability(getHoldability())
                .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return HivePreparedStatement.preparedStatementBuilder().connection(this).sql(sql).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return HivePreparedStatement.preparedStatementBuilder().connection(this).sql(sql).type(resultSetType).concurrency(resultSetConcurrency).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return HivePreparedStatement.preparedStatementBuilder().connection(this).sql(sql).type(resultSetType).concurrency(resultSetConcurrency).holdability(resultSetHoldability).build();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return HiveDatabaseMetaData.builder().connection(this).build();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // no-op; don't support setting this value
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return HiveStatement.builder()
                .connection(this)
                .type(resultSetType)
                .concurrency(resultSetConcurrency)
                .holdability(getHoldability())
                .build();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return HiveStatement.builder()
                .connection(this)
                .type(resultSetType)
                .concurrency(resultSetConcurrency)
                .holdability(resultSetHoldability)
                .build();
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        // no-op; don't support setting this value
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // no-op; connection does not use
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // no-op; no catalog in hive
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // no-op; don't support transactions yet
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // no-op; need to better understand how this differs from DriverManager.getLoginTimeout()
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public String getSchema() throws SQLException {
        return QueryUtils.getDatabaseSchema(this);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        QueryUtils.setDatabaseSchema(this, schema);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLDataException(MessageFormat.format("timeout must be greater than or equal to 0.  Current value is {0}", timeout));
        }

        return QueryUtils.isValid(this, timeout);
    }

    @Override
    public void abort(Executor executor) throws SQLException {

        if (thriftSession.isClosed()) {
            return;
        }

        SQL_PERMISSION_ABORT.checkGuard(this);

        AbortCommand command = new AbortCommand();
        if (executor != null) {
            executor.execute(command);
        } else {
            command.run();
        }
    }

    public static class HiveConnectionBuilder implements Builder<HiveConnection> {

        private Properties properties;

        private ThriftTransport thriftTransport;

        private HiveConnectionBuilder() {
        }

        public HiveConnectionBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public HiveConnectionBuilder thriftTransport(ThriftTransport thriftTransport) {
            this.thriftTransport = thriftTransport;
            return this;
        }

        public HiveConnection build() {
            ThriftSession thriftSession = ThriftSession.builder()
                    .properties(properties)
                    .thriftTransport(thriftTransport)
                    .build();

            return new HiveConnection(thriftSession);
        }


    }

    public class AbortCommand implements Runnable {
        public void run() {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("attempting to close from abort command");
                }
                close();
            } catch (SQLException e) {
                log.error(MessageFormat.format("error closing during abort: sql state [{0}]", e.getSQLState()), e);
            }
        }
    }
}
