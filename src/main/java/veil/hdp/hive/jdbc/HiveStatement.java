package veil.hdp.hive.jdbc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

public class HiveStatement extends AbstractStatement {

    private static final Logger log = LoggerFactory.getLogger(HiveStatement.class);

    // constructor
    private final HiveConnection connection;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    // private
    private ThriftOperation thriftOperation;

    private ResultSet resultSet;

    // public getter & setter
    private int queryTimeout;
    private int maxRows;
    private int fetchSize;
    private SQLWarning sqlWarning;


    private HiveStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.connection = connection;

        this.queryTimeout = 0;
        this.maxRows = 0;
        this.fetchSize = 1000;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

    }

    private void performThriftOperation(String sql) throws SQLException {
        thriftOperation = new ThriftOperation.Builder().statement(this).sql(sql).timeout(queryTimeout).build();

        resultSet = new HiveResultSet.Builder().statement(this).operation(thriftOperation).build();

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        performThriftOperation(sql);

        return thriftOperation.hasResultSet();

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {

        performThriftOperation(sql);

        if (!thriftOperation.hasResultSet()) {
            throw new SQLException("The query did not generate a result set!");
        }

        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {

        performThriftOperation(sql);

        if (thriftOperation.hasResultSet()) {
            throw new SQLException("The query generated a result set when an updated was expected");
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
        return resultSet;
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
        thriftOperation.cancel();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return thriftOperation.isClosed();
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
        return thriftOperation.getModifiedCount();
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
        thriftOperation.close();

        if (resultSet != null) {
            resultSet.close();
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
        return HiveServiceUtils.getGeneratedKeys(connection);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // no-op; don't support setting this value
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
