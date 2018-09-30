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
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftOperation implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(ThriftOperation.class);


    private static final short FETCH_TYPE_QUERY = 0;
    private static final short FETCH_TYPE_LOG = 1;

    // constructor
    private final TCLIService.Iface client;
    private final TOperationHandle operationHandle;
    private final boolean hasResultSet;
    private final Schema schema;
    private final int modifiedCount;
    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private ThriftOperation(TCLIService.Iface client, TOperationHandle operationHandle, Schema schema, boolean hasResultSet, int modifiedCount) {

        this.client = client;
        this.operationHandle = operationHandle;
        this.schema = schema;
        this.hasResultSet = hasResultSet;
        this.modifiedCount = modifiedCount;

        closed.set(false);
    }

    public static ThriftOperationBuilder builder() {
        return new ThriftOperationBuilder();
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

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            log.trace("attempting to close {}", this.getClass().getName());

            try {
                closeOperation();
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }
    }

    public void cancel() {
        if (!closed.get()) {

            log.trace("attempting to cancel {}", this.getClass().getName());

            cancelOperation();
        }
    }


    private void cancelOperation() {

        try {

            TCancelOperationReq cancelRequest = new TCancelOperationReq(operationHandle);

            TCancelOperationResp resp = client.CancelOperation(cancelRequest);


            if (resp != null) {
                try {
                    ThriftUtils.checkStatus(resp.getStatus());
                } catch (HiveThriftException e) {
                    log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
                }
            }

        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }


    }


    private TRowSet getRowSet(TFetchResultsReq tFetchResultsReq) {

        try {

            TFetchResultsResp fetchResults = client.FetchResults(tFetchResultsReq);

            ThriftUtils.checkStatus(fetchResults.getStatus());

            return fetchResults.getResults();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }
    }


    public ColumnBasedSet fetchResults(TFetchOrientation orientation, int fetchSize) {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, orientation, fetchSize);
        fetchReq.setFetchType(FETCH_TYPE_QUERY);

        TRowSet tRowSet = getRowSet(fetchReq);

        return convertToCBS(tRowSet);
    }

    /*private static List<Row> fetchLogs(ThriftOperation operation, int fetchSize) {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operation.getOperationHandle(), TFetchOrientation.FETCH_FIRST, fetchSize);
        fetchReq.setFetchType(FETCH_TYPE_LOG);

        TRowSet tRowSet = getRowSet(operation, fetchReq);

        return convertTRowSet(operation, tRowSet);

    }*/

    private ColumnBasedSet convertToCBS(TRowSet rowSet) {
        if (rowSet != null && rowSet.isSetColumns()) {
            if (!rowSet.getColumns().isEmpty()) {
                return ColumnBasedSet.builder().rowSet(rowSet).schema(schema).build();
            }
        }

        return null;
    }


    private void closeOperation() {

        try {

            TCloseOperationReq closeRequest = new TCloseOperationReq(operationHandle);

            TCloseOperationResp resp = client.CloseOperation(closeRequest);

            if (resp != null) {
                try {
                    ThriftUtils.checkStatus(resp.getStatus());
                } catch (HiveThriftException e) {
                    log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
                }
            }

        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }


    }

    public static class ThriftOperationBuilder implements Builder<ThriftOperation> {

        private TOperationHandle operationHandle;
        private TCLIService.Iface client;


        private ThriftOperationBuilder() {
        }

        public ThriftOperationBuilder client(TCLIService.Iface client) {
            this.client = client;
            return this;
        }


        public ThriftOperationBuilder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public ThriftOperation build() {

            boolean hasResultSet = false;
            int modifiedCount = -1;
            Schema schema = null;

            if (operationHandle.isSetHasResultSet()) {
                hasResultSet = operationHandle.isHasResultSet();
            }

            if (hasResultSet) {
                schema = Schema.builder().client(client).handle(operationHandle).build();
            } else {

                // return actual modified count or zero since -1 is not a valid value when no result set exists
                modifiedCount = 0;

                if (operationHandle.isSetModifiedRowCount()) {
                    modifiedCount = (int) operationHandle.getModifiedRowCount();
                }
            }

            return new ThriftOperation(client, operationHandle, schema, hasResultSet, modifiedCount);
        }

    }

}
