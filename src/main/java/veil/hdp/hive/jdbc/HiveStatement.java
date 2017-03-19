package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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

    // public getter & setter
    private int queryTimeout;
    private int maxRows;
    private int fetchSize;
    private int fetchDirection;

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
        this.fetchDirection = ResultSet.FETCH_FORWARD;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency  = resultSetConcurrency;
        this.resultSetHoldability  = resultSetHoldability;

        resultSet = null;
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        closeStatementHandle();

        try {
            statementHandle = HiveServiceUtils.executeSql(connection.getClient(), connection.getSessionHandle(), queryTimeout, sql);
            HiveServiceUtils.waitForStatementToComplete(connection.getClient(), statementHandle);

            if (!statementHandle.isHasResultSet()) {
                return false;
            }

            TableSchema tableSchema = new TableSchema(HiveServiceUtils.getResultSetSchema(connection.getClient(), statementHandle));

            if (log.isDebugEnabled()) {
                log.debug(tableSchema.toString());
            }

            resultSet = new HiveResultSet(connection, this, statementHandle, tableSchema);

        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }

        return true;
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
        execute(sql);

        return 0;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
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
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
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
