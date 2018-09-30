/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.thrift.ThriftOperation;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.QueryUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveStatement extends AbstractStatement {

    private static final Logger log = LogManager.getLogger(HiveStatement.class);

    // constructor
    private final HiveConnection connection;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    // private
    private ThriftOperation thriftOperation = null;
    // public getter & setter
    private int queryTimeout;
    private int maxRows;
    private int fetchSize;
    private int fetchDirection;
    private SQLWarning sqlWarning;
    private int updateCount = -1;
    private ResultSet resultSet;


    HiveStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.connection = connection;

        this.queryTimeout = Constants.DEFAULT_QUERY_TIMEOUT;
        this.maxRows = Constants.DEFAULT_MAX_ROWS;
        this.fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());
        this.fetchDirection = ResultSet.FETCH_FORWARD;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

        closed.set(false);
    }

    public static HiveStatementBuilder builder() {
        return new HiveStatementBuilder();
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        if (thriftOperation != null) {
            close();
        }

        thriftOperation = ThriftUtils.executeSql(connection.getThriftSession(), sql, queryTimeout);

        if (thriftOperation.hasResultSet()) {

            resultSet = HiveResultSet.builder()
                    .thriftOperation(thriftOperation)
                    .statement(this)
                    .resultSetConcurrency(resultSetConcurrency)
                    .resultSetHoldability(resultSetHoldability)
                    .resultSetType(resultSetType)
                    .fetchDirection(fetchDirection)
                    .fetchSize(fetchSize)
                    .maxRows(maxRows)
                    .build();

            return true;
        } else {
            updateCount = thriftOperation.getModifiedCount();

            return false;
        }


    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        boolean result = execute(sql);

        if (!result) {
            throw new HiveSQLException("The query did not generate a result set!");
        }

        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {

        boolean result = execute(sql);

        if (result) {
            throw new HiveSQLException("The query generated a result set when an updated was expected");
        }

        return updateCount;
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

        if (isClosed()) {
            throw new HiveSQLException("Cannot 'cancel' Statement.  Connection is closed.");
        }

        if (thriftOperation != null) {
            thriftOperation.cancel();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed.get();
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
        return updateCount;
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

        if (closed.compareAndSet(false, true)) {

            log.trace("attempting to close {}", this.getClass().getName());

            if (thriftOperation != null && !thriftOperation.isClosed()) {
                DriverUtils.closeAndNull(thriftOperation);
            }

            if (resultSet != null && !resultSet.isClosed()) {
                DriverUtils.closeAndNull(resultSet);
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // no-op; don't support setting this value
        log.warn("no-op for method setMaxFieldSize()");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // no-op; don't support setting this value
        log.warn("no-op for method setEscapeProcessing()");
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
        log.warn("no-op for method setPoolable()");
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
        log.warn("no-op for method closeOnCompletion()");
    }

    public static class HiveStatementBuilder implements Builder<HiveStatement> {

        HiveConnection connection;
        int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        HiveStatementBuilder() {
        }

        HiveStatementBuilder connection(HiveConnection connection) {
            this.connection = connection;
            return this;
        }

        HiveStatementBuilder type(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        HiveStatementBuilder concurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        HiveStatementBuilder holdability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }


        public HiveStatement build() {
            return new HiveStatement(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }

}
