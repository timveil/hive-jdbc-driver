package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.metadata.TableSchema;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveStatement extends AbstractStatement {

    private static final Logger log = LoggerFactory.getLogger(HiveStatement.class);

    // constructor
    private final HiveConnection connection;

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
        this.connection = connection;

        this.queryTimeout = 0;
        this.maxRows = 0;
        this.fetchSize = 1000;
        this.fetchDirection = ResultSet.FETCH_FORWARD;

        resultSet = null;
    }

    TOperationHandle getStatementHandle() {
        return statementHandle;
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

            TableSchema tableSchema = new TableSchema(HiveServiceUtils.getSchema(connection.getClient(), statementHandle));

            if (log.isDebugEnabled()) {
                log.debug(tableSchema.toString());
            }

            resultSet = new HiveResultSet(connection, this, tableSchema);
            resultSet.setFetchSize(fetchSize);
            resultSet.setFetchDirection(fetchDirection);

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
        return ResultSet.TYPE_FORWARD_ONLY;
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
        if (resultSet != null) {
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
