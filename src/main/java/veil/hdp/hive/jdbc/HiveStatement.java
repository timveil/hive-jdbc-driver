package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
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
    private TOperationHandle statementHandle;
    private ResultSet resultSet;
    private double modifiedRowCount = 0;

    // public getter & setter
    private int queryTimeout;
    private int maxRows;
    private int fetchSize;
    private SQLWarning sqlWarning;

    // public getter only
    private boolean closed;

    HiveStatement(HiveConnection connection) {
        this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    HiveStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.connection = connection;

        this.queryTimeout = 0;
        this.maxRows = 0;
        this.fetchSize = 1000;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

        resultSet = null;
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        closeStatementHandle();

        statementHandle = HiveServiceUtils.executeSql(connection.getClient(), connection.getSessionHandle(), queryTimeout, sql);
        HiveServiceUtils.waitForStatementToComplete(connection.getClient(), statementHandle);

        if (statementHandle.isHasResultSet()) {

            Schema schema = new Schema(HiveServiceUtils.getResultSetSchema(connection.getClient(), statementHandle));

            if (log.isDebugEnabled()) {
                log.debug(schema.toString());
            }

            resultSet = new HiveResultSet(connection, this, statementHandle, schema);

            return true;

        } else {
            modifiedRowCount = statementHandle.getModifiedRowCount();

            return false;
        }


    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException("The query did not generate a result set!");
        }
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (execute(sql)) {
            log.warn("The following sql generated a result set when an update was called, something seems amiss.  sql [{}]", sql);
        }

        return new Double(modifiedRowCount).intValue();
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
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
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
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public void cancel() throws SQLException {
        HiveServiceUtils.cancelOperation(connection.getClient(), statementHandle);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
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
        return resultSet != null ? -1 : new Double(modifiedRowCount).intValue();
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

        if (!closed) {

            if (log.isDebugEnabled()) {
                log.debug("attempting to close {}", this.getClass().getName());
            }

            closeStatementHandle();

            closeResultSet();

            closed = true;
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

    private void closeResultSet() throws SQLException {
        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
            resultSet = null;
        }
    }

    private void closeStatementHandle() {
        if (statementHandle != null) {
            HiveServiceUtils.closeOperation(connection.getClient(), statementHandle);
            statementHandle = null;
        }
    }


}
