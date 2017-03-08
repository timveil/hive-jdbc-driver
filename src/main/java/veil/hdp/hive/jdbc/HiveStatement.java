package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveStatement extends AbstractStatement {

    private static final Logger log = LoggerFactory.getLogger(HiveStatement.class);

    // constructor
    private final HiveConnection connection;
    private TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private boolean isScrollableResultSet = false;
    private TProtocolVersion protocolVersion;

    // private
    private TOperationHandle statementHandle;

    // public getter & setter
    private int queryTimeout = 0;
    private int maxRows = 0;
    // lets handle defaults better
    private int fetchSize = 1000;
    private ResultSet resultSet;
    private boolean isClosed = false;


    public HiveStatement(HiveConnection connection, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this(connection, client, sessionHandle, protocolVersion, false);
    }

    public HiveStatement(HiveConnection connection, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion, boolean isScrollableResultSet) {
        this.connection = connection;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocolVersion = protocolVersion;
        this.isScrollableResultSet = isScrollableResultSet;
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        try {
            statementHandle = HiveServiceUtils.executeSql(client, sessionHandle, queryTimeout, sql);
            HiveServiceUtils.waitForStatementToComplete(client, statementHandle);
        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }


        if (!statementHandle.isHasResultSet()) {
            return false;
        }

        try {
            resultSet = new HiveQueryResultSet(client, statementHandle, protocolVersion, this, isScrollableResultSet, maxRows);
            //todo: should fetch size be part of statement constructor
            resultSet.setFetchSize(fetchSize);
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
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.maxRows;
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
       return this.fetchSize;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.resultSet;
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
        HiveServiceUtils.cancelOperation(client, statementHandle);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
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
       HiveServiceUtils.closeOperation(client, statementHandle);
       client = null;
       statementHandle = null;

       resultSet.close();
       resultSet= null;

       isClosed = true;
    }
}
