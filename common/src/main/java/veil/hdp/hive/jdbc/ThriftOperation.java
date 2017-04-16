package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.QueryUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    // constructor
    private final ThriftSession session;
    private final TOperationHandle operation;
    private final ResultSet resultSet;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(ThriftSession thriftSession, TOperationHandle operationHandle, ResultSet resultSet) {

        this.session = thriftSession;
        this.operation = operationHandle;
        this.resultSet = resultSet;

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

    public ResultSet getResultSet() {
        return resultSet;
    }


    public int getModifiedCount() {
        if (operation.isSetModifiedRowCount()) {
            return new Double(operation.getModifiedRowCount()).intValue();
        }

        return -1;
    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {
            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            QueryUtils.closeOperation(this);

            resultSet.close();

        }
    }

    public void cancel() {
        if (!closed.get()) {
            QueryUtils.cancelOperation(this);
        }
    }

    public static class Builder {

        private TOperationHandle operationHandle;
        private ThriftSession session;
        private int maxRows = Constants.DEFAULT_MAX_ROWS;
        private int fetchSize = Constants.DEFAULT_FETCH_SIZE;
        private int fetchDirection = ResultSet.FETCH_FORWARD;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        private int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        private boolean metaDataOperation;

        public ThriftOperation.Builder session(ThriftSession session) {
            this.session = session;
            return this;
        }


        public ThriftOperation.Builder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public ThriftOperation.Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }


        public ThriftOperation.Builder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public ThriftOperation.Builder fetchDirection(int fetchDirection) {
            this.fetchDirection = fetchDirection;
            return this;
        }


        public ThriftOperation.Builder resultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }


        public ThriftOperation.Builder resultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }


        public ThriftOperation.Builder resultSetHoldability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }

        public ThriftOperation.Builder metaData(boolean metaDataOperation) {
            this.metaDataOperation = metaDataOperation;
            return this;
        }

        public ThriftOperation build() throws SQLException {

            ResultSet resultSet;

            if (operationHandle.isHasResultSet()) {

                if (metaDataOperation) {
                    resultSet = new HiveMetaDataResultSet.Builder()
                            .handle(operationHandle)
                            .thriftSession(session)
                            .build();
                } else {
                    resultSet = new HiveResultSet.Builder()
                            .thriftSession(session)
                            .handle(operationHandle)
                            .resultSetConcurrency(resultSetConcurrency)
                            .resultSetHoldability(resultSetHoldability)
                            .maxRows(maxRows)
                            .fetchSize(fetchSize)
                            .fetchDirection(fetchDirection)
                            .resultSetType(resultSetType)
                            .build();
                }

            } else {
                resultSet = new HiveEmptyResultSet.Builder().build();
            }

            return new ThriftOperation(session, operationHandle, resultSet);

        }

    }

}
