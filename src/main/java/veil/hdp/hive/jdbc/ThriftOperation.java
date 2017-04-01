package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    private final TCLIService.Client client;
    private final TOperationHandle operationHandle;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(TCLIService.Client client, TOperationHandle operationHandle) {

        this.client = client;
        this.operationHandle = operationHandle;

        closed.set(false);
    }

    public TCLIService.Client getClient() {
        return client;
    }

    public TOperationHandle getOperationHandle() {
        return operationHandle;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean hasResultSet() {
        return operationHandle.isHasResultSet();
    }

    public int getModifiedCount() {
        if (operationHandle.isSetModifiedRowCount()) {
            return new Double(operationHandle.getModifiedRowCount()).intValue();
        }

        return -1;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("attempting to close {}", this.getClass().getName());
            }

            HiveServiceUtils.closeOperation(client, operationHandle);
        }
    }

    public void cancel() {
        if (!closed.get()) {
            HiveServiceUtils.cancelOperation(client, operationHandle);
        }
    }

    @Override
    public String toString() {
        return "ThriftOperation{" +
                "client=" + client +
                ", operationHandle=" + operationHandle +
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

            TCLIService.Client client = thriftSession.getClient();

            TOperationHandle operationHandle = HiveServiceUtils.executeSql(client, thriftSession.getSessionHandle(), queryTimeout, sql);

            HiveServiceUtils.waitForStatementToComplete(client, operationHandle);

            return new ThriftOperation(client, operationHandle);

        }

    }

}
