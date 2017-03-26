package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    private final TCLIService.Client client;
    private final TOperationHandle operationHandle;
    private final HiveResultSet resultSet;
    private final int modifiedRowCount;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(TCLIService.Client client, TOperationHandle operationHandle, HiveResultSet resultSet, int modifiedRowCount) {

        this.client = client;
        this.operationHandle = operationHandle;
        this.resultSet = resultSet;
        this.modifiedRowCount = modifiedRowCount;

        closed.set(false);
    }


    public TOperationHandle getOperationHandle() {
        return operationHandle;
    }

    public HiveResultSet getResultSet() {
        return resultSet;
    }

    public int getModifiedRowCount() {
        return modifiedRowCount;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean hasResultSet() {
        return operationHandle.isHasResultSet();
    }

    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {
            HiveServiceUtils.closeOperation(client, operationHandle);
            resultSet.close();
        }
    }

    public void cancel() throws SQLException {
        if (!closed.get()) {
            HiveServiceUtils.cancelOperation(client, operationHandle);
        }
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

            HiveConnection connection = (HiveConnection) hiveStatement.getConnection();

            ThriftSession thriftSession = connection.getThriftSession();

            TCLIService.Client client = thriftSession.getClient();

            TOperationHandle operationHandle = HiveServiceUtils.executeSql(client, thriftSession.getSessionHandle(), queryTimeout, sql);

            HiveServiceUtils.waitForStatementToComplete(client, operationHandle);

            HiveResultSet resultSet = null;
            if (operationHandle.isHasResultSet()) {
                Schema schema = new Schema(HiveServiceUtils.getResultSetSchema(client, operationHandle));
                resultSet = new HiveResultSet(hiveStatement, operationHandle, schema);
            }

            int modifiedRowCount = -1;
            if (operationHandle.isSetModifiedRowCount()) {
                modifiedRowCount = new Double(operationHandle.getModifiedRowCount()).intValue();
            }

            return new ThriftOperation(client, operationHandle, resultSet, modifiedRowCount);

        }

    }

}
