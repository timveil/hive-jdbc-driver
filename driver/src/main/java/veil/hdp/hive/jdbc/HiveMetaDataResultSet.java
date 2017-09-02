package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.bindings.TOperationHandle;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.SQLException;
import java.util.Iterator;

public class HiveMetaDataResultSet extends HiveBaseResultSet {

    private static final Logger log =  LogManager.getLogger(HiveMetaDataResultSet.class);

    // constructor
    private final ThriftSession session;
    private final TOperationHandle operationHandle;
    private final Iterator<Row> results;


    private HiveMetaDataResultSet(ThriftSession session, TOperationHandle operationHandle, Schema schema, Iterator<Row> results) {

        super(schema);

        this.session = session;
        this.operationHandle = operationHandle;
        this.results = results;

    }

    public static HiveMetaDataResultSetBuilder builder() {
        return new HiveMetaDataResultSetBuilder();
    }

    @Override
    public boolean next() throws SQLException {
        if (!results.hasNext()) {
            currentRow.set(null);
            return false;
        }

        currentRow.set(results.next());

        return true;

    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            ThriftUtils.closeOperation(session, operationHandle);

            currentRow.set(null);

        }
    }

    public static class HiveMetaDataResultSetBuilder implements Builder<HiveMetaDataResultSet> {


        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;
        private int fetchSize;

        private HiveMetaDataResultSetBuilder() {
        }

        public HiveMetaDataResultSetBuilder thriftSession(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public HiveMetaDataResultSetBuilder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }

        public HiveMetaDataResultSetBuilder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public HiveMetaDataResultSet build() {

            Schema schema = Schema.builder().session(thriftSession).handle(operationHandle).build();

            Iterable<Row> results = ThriftUtils.getResults(thriftSession, operationHandle, fetchSize, schema);

            return new HiveMetaDataResultSet(thriftSession, operationHandle, schema, results.iterator());
        }
    }


}
