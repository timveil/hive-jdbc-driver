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
    private final boolean hasResultSet;
    private final int modifiedCount;
    private final String operationType;
    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private TOperationHandle operationHandle;
    private Schema schema;

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
    public void close() {
        if (closed.compareAndSet(false, true)) {

            log.trace("attempting to close {}", this.getClass().getName());

            try {
                ThriftUtils.closeOperation(session, operationHandle);
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            } finally {
                operationHandle = null;
                schema = null;
            }
        }
    }

    public void cancel() {
        if (!closed.get()) {

            log.trace("attempting to cancel {}", this.getClass().getName());

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
                hasResultSet = operationHandle.isHasResultSet();
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

            Schema schema = null;

            // if operation is something like EXECUTE_STATEMENT and doesn't have a result set, then schema building can fail
            if (hasResultSet) {
                schema = Schema.builder().session(session).handle(operationHandle).build();
            }

            return new ThriftOperation(session, operationHandle, schema, hasResultSet, modifiedCount, operation);
        }

    }

}
