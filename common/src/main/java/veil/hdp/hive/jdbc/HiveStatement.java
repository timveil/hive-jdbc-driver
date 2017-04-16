package veil.hdp.hive.jdbc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.QueryUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.atomic.AtomicReference;

public class HiveStatement extends AbstractStatement {

    protected static final Logger log = LoggerFactory.getLogger(HiveStatement.class);

    // constructor
    private final HiveConnection connection;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    // private
    private final AtomicReference<ThriftOperation> currentOperation = new AtomicReference<>();

    // public getter & setter
    private int queryTimeout;
    private int maxRows;
    private int fetchSize;
    private SQLWarning sqlWarning;


    HiveStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.connection = connection;

        this.queryTimeout = Constants.DEFAULT_QUERY_TIMEOUT;
        this.maxRows = Constants.DEFAULT_MAX_ROWS;
        this.fetchSize = Constants.DEFAULT_FETCH_SIZE;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

    }

    private void performThriftOperation(String sql) throws SQLException {

        ThriftOperation operation = currentOperation.get();

        if (operation != null) {
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! closing current thriftOperation !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            operation.close();
            currentOperation.compareAndSet(operation, null);
        }

        currentOperation.compareAndSet(null, QueryUtils.executeSql(connection.getThriftSession(), sql, queryTimeout, fetchSize, maxRows));
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        performThriftOperation(sql);

        return currentOperation.get().hasResultSet();

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        performThriftOperation(sql);

        ThriftOperation thriftOperation = currentOperation.get();

        if (!thriftOperation.hasResultSet()) {
            throw new HiveSQLException("The query did not generate a result set!");
        }

        return thriftOperation.getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {

        performThriftOperation(sql);

        ThriftOperation thriftOperation = currentOperation.get();

        if (thriftOperation.hasResultSet()) {
            throw new HiveSQLException("The query generated a result set when an updated was expected");
        }

        return thriftOperation.getModifiedCount();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // no-op; don't support setting a different direction
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentOperation.get().getResultSet();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override
    public HiveConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public void cancel() throws SQLException {
        ThriftOperation operation = currentOperation.get();

        if (operation != null) {
            operation.cancel();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        ThriftOperation operation = currentOperation.get();

        return operation == null || operation.isClosed();

    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ThriftOperation operation = currentOperation.get();

        if (operation != null) {
            return operation.getModifiedCount();
        }

        return -1;
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
    public void close() throws SQLException {

        ThriftOperation operation = currentOperation.get();

        if (operation != null) {
            operation.close();
            currentOperation.set(null);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // no-op; don't support setting this value
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // no-op; don't support setting this value
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // no-op; don't support pooling statements
        // todo: should consider pooling statements
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return QueryUtils.getGeneratedKeys(connection);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // no-op; don't support setting this value
    }

    @Override
    public String toString() {
        return "HiveStatement{" +
                "connection=" + connection +
                ", resultSetType=" + resultSetType +
                ", resultSetConcurrency=" + resultSetConcurrency +
                ", resultSetHoldability=" + resultSetHoldability +
                ", queryTimeout=" + queryTimeout +
                ", maxRows=" + maxRows +
                ", fetchSize=" + fetchSize +
                ", sqlWarning=" + sqlWarning +
                '}';
    }

    public static class Builder {

        private HiveConnection connection;
        private int resultSetType;
        private int resultSetConcurrency;
        private int resultSetHoldability;

        public HiveStatement.Builder connection(HiveConnection connection) {
            this.connection = connection;
            return this;
        }

        public HiveStatement.Builder type(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        public HiveStatement.Builder concurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public HiveStatement.Builder holdability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }


        public HiveStatement build() {
            return new HiveStatement(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }

}
