package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    private final ThriftSession session;
    private final TOperationHandle operation;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(ThriftSession thriftSession, TOperationHandle operationHandle) {

        this.session = thriftSession;
        this.operation = operationHandle;


        closed.set(false);
    }

    public ThriftSession getSession() {
        return session;
    }

    public TOperationHandle getOperationHandle() {
        return operation;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean hasResultSet() {
        return operation.isHasResultSet();
    }

    public int getModifiedCount() {
        if (operation.isSetModifiedRowCount()) {
            return new Double(operation.getModifiedRowCount()).intValue();
        }

        return -1;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            QueryService.closeOperation(this);

        }
    }

    public void cancel() {
        if (!closed.get()) {
            QueryService.cancelOperation(this);
        }
    }

    @Override
    public String toString() {
        return "ThriftOperation{" +
                ", closed=" + closed +
                '}';
    }

    public static class Builder {

        private String sql;

        private int queryTimeout;

        private HiveStatement hiveStatement;

        public ThriftOperation.Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public ThriftOperation.Builder timeout(int queryTimeout) {
            this.queryTimeout = queryTimeout;
            return this;
        }

        public ThriftOperation.Builder statement(HiveStatement hiveStatement) {
            this.hiveStatement = hiveStatement;
            return this;
        }

        public ThriftOperation build() throws SQLException {

            HiveConnection connection = hiveStatement.getConnection();

            ThriftSession thriftSession = connection.getThriftSession();



            TOperationHandle operationHandle = QueryService.executeSql(thriftSession, queryTimeout, sql);

            QueryService.waitForStatementToComplete(thriftSession, operationHandle);

            return new ThriftOperation(thriftSession, operationHandle);

        }

    }

}
