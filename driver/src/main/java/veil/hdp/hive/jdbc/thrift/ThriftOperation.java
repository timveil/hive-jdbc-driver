package veil.hdp.hive.jdbc.thrift;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.TOperationHandle;
import veil.hdp.hive.jdbc.bindings.TOperationType;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(ThriftOperation.class);

    // constructor
    private final ThriftSession session;
    private TOperationHandle operationHandle;
    private Schema schema;
    private final boolean hasResultSet;
    private final int modifiedCount;
    private final String operationType;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(ThriftSession thriftSession, TOperationHandle operationHandle, Schema schema, boolean hasResultSet, int modifiedCount, String operationType) {

        this.session = thriftSession;
        this.operationHandle = operationHandle;
        this.schema = schema;
        this.hasResultSet = hasResultSet;
        this.modifiedCount = modifiedCount;
        this.operationType = operationType;

        closed.set(false);
    }

    public static ThriftOperationBuilder builder() {
        return new ThriftOperationBuilder();
    }

    public ThriftSession getSession() {
        return session;
    }

    public TOperationHandle getOperationHandle() {
        return operationHandle;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean hasResultSet() {
        return hasResultSet;
    }

    public int getModifiedCount() {
        return modifiedCount;
    }

    public String getOperationType() {
        return operationType;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            ThriftUtils.closeOperation(session, operationHandle);

            operationHandle = null;
            schema = null;

        }
    }

    public void cancel() {
        if (!closed.get()) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to cancel {}", this.getClass().getName());
            }

            ThriftUtils.cancelOperation(this);
        }
    }

    public static class ThriftOperationBuilder implements Builder<ThriftOperation> {

        private TOperationHandle operationHandle;
        private ThriftSession session;


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


        public ThriftOperation build() {

            boolean hasResultSet = false;

            if (operationHandle.isSetHasResultSet()) {
                hasResultSet = operationHandle.isSetHasResultSet();
            }

            int modifiedCount = -1;

            if (operationHandle.isSetModifiedRowCount()) {
                modifiedCount = (int) operationHandle.getModifiedRowCount();
            }

            String operation = null;

            if (operationHandle.isSetOperationType()) {
                TOperationType operationType = operationHandle.getOperationType();

                operation = operationType.name();
            }

            Schema schema = Schema.builder().session(session).handle(operationHandle).build();

            return new ThriftOperation(session, operationHandle, schema, hasResultSet, modifiedCount, operation);
        }

    }

}
