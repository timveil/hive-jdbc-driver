package veil.hdp.hive.jdbc.thrift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveEmptyResultSet;
import veil.hdp.hive.jdbc.HiveMetaDataResultSet;
import veil.hdp.hive.jdbc.HiveResultSet;
import veil.hdp.hive.jdbc.bindings.TOperationHandle;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ThriftOperation.class);

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

    public static ThriftOperationBuilder builder() {
        return new ThriftOperationBuilder();
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            ThriftUtils.closeOperation(this);

            try {
                resultSet.close();
            } catch (SQLException e) {
                log.warn(e.getMessage(), e);
            }
        }
    }

    public void cancel() {
        if (!closed.get()) {
            ThriftUtils.cancelOperation(this);
        }
    }

    public static class ThriftOperationBuilder implements Builder<ThriftOperation> {

        private TOperationHandle operationHandle;
        private ThriftSession session;
        private int maxRows = Constants.DEFAULT_MAX_ROWS;
        private int fetchSize = Constants.DEFAULT_FETCH_SIZE;
        private int fetchDirection = ResultSet.FETCH_FORWARD;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        private int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        private boolean metaDataOperation;

        private ThriftOperationBuilder() {
        }

        public ThriftOperationBuilder session(ThriftSession session) {
            this.session = session;
            return this;
        }


        public ThriftOperationBuilder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public ThriftOperationBuilder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }


        public ThriftOperationBuilder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public ThriftOperationBuilder fetchDirection(int fetchDirection) {
            this.fetchDirection = fetchDirection;
            return this;
        }


        public ThriftOperationBuilder resultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }


        public ThriftOperationBuilder resultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }


        public ThriftOperationBuilder resultSetHoldability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }

        public ThriftOperationBuilder metaData(boolean metaDataOperation) {
            this.metaDataOperation = metaDataOperation;
            return this;
        }

        public ThriftOperation build() {

            ResultSet resultSet;

            if (operationHandle.isHasResultSet()) {

                if (metaDataOperation) {
                    resultSet = HiveMetaDataResultSet.builder()
                            .handle(operationHandle)
                            .thriftSession(session)
                            .build();
                } else {
                    resultSet = HiveResultSet.builder()
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
                resultSet = HiveEmptyResultSet.builder().build();
            }

            return new ThriftOperation(session, operationHandle, resultSet);

        }

    }

}
