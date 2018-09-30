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
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.QueryUtils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveConnection extends AbstractConnection {

    private static final Logger log = LogManager.getLogger(HiveConnection.class);

    private static final SQLPermission SQL_PERMISSION_ABORT = new SQLPermission("callAbort");

    // constructor
    private ThriftSession thriftSession;
    private final AtomicBoolean closed = new AtomicBoolean(true);

    // public getter & setter
    private SQLWarning sqlWarning;

    private HiveConnection(ThriftSession thriftSession) {
        this.thriftSession = thriftSession;

        closed.set(false);
    }

    public static HiveConnectionBuilder builder() {
        return new HiveConnectionBuilder();
    }

    public ThriftSession getThriftSession() {
        return thriftSession;
    }

    @Override
    public void close() throws SQLException {

        if (closed.compareAndSet(false, true)) {

            log.trace("attempting to close {}", this.getClass().getName());

            DriverUtils.close(thriftSession);

        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed.get();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return HiveStatement.builder()
                .connection(this)
                .holdability(getHoldability())
                .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return HivePreparedStatement.builder().connection(this).sql(sql).holdability(getHoldability()).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return HivePreparedStatement.builder().connection(this).sql(sql).type(resultSetType).concurrency(resultSetConcurrency).holdability(getHoldability()).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return HivePreparedStatement.builder().connection(this).sql(sql).type(resultSetType).concurrency(resultSetConcurrency).holdability(resultSetHoldability).build();
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
        log.warn("no-op for method setAutoCommit()");
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
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw HiveDriver.notImplemented(this.getClass(), "setHoldability", "holdability is not supported");
        }

        // no-op; don't support setting this value
        log.warn("no-op for method setHoldability()");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // no-op; connection does not use
        log.warn("no-op for method setReadOnly()");
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // no-op; no catalog in hive
        log.warn("no-op for method setCatalog()");
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
        log.warn("no-op for method setTransactionIsolation()");
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

        return thriftSession != null && thriftSession.isValid();
    }

    @Override
    public void abort(Executor executor) throws SQLException {

        if (isClosed()) {
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

        private HiveConnectionBuilder() {
        }

        public HiveConnectionBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }


        public HiveConnection build() {

            ThriftSession thriftSession = ThriftSession.builder()
                    .properties(properties)
                    .build();

            return new HiveConnection(thriftSession);
        }


    }

    public class AbortCommand implements Runnable {
        public void run() {
            try {
                log.trace("attempting to close from abort command");
                close();
            } catch (SQLException e) {
                log.error(MessageFormat.format("error closing during abort: sql state [{0}]", e.getSQLState()), e);
            }
        }
    }
}
