package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOperationHandle;
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


    private TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private boolean isScrollableResultSet = false;

    private final HiveConnection connection;
    private int queryTimeout = 0;
    private int maxRows = 0;
    private int fetchSize = 1000;
    private ResultSet resultSet;

    ////////////////////////////











    public HiveStatement(HiveConnection connection, TCLIService.Client client, TSessionHandle sessionHandle) {
        this(connection, client, sessionHandle, false);
    }

    public HiveStatement(HiveConnection connection, TCLIService.Client client, TSessionHandle sessionHandle, boolean isScrollableResultSet) {
        this.connection = connection;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.isScrollableResultSet = isScrollableResultSet;
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        try {
            return HiveServiceUtils.executeSql(client, sessionHandle, queryTimeout, sql, new OperationHandleCallback() {
                @Override
                public void process(TOperationHandle statementHandle) {
                  /*  resultSet = new HiveQueryResultSet.Builder(this)
                            .setClient(client)
                            .setSessionHandle(sessionHandle)
                            .setStmtHandle(statementHandle)
                            .setMaxRows(maxRows)
                            .setFetchSize(fetchSize)
                            .setScrollable(isScrollableResultSet)
                            .build();*/
                }
            });
        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }


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
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public void close() throws SQLException {
       //HiveServiceUtils.closeOperation(client, );
    }
}
