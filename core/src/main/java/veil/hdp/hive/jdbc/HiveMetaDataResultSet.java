package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.TOperationHandle;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.SQLException;
import java.util.Iterator;

public class HiveMetaDataResultSet extends HiveBaseResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveMetaDataResultSet.class);

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

            ThriftUtils.closeOperation(session, operationHandle);

            if (schema != null) {
                schema.clear();
            }

            currentRow.set(null);

        }
    }

    public static class HiveMetaDataResultSetBuilder implements Builder<HiveMetaDataResultSet> {


        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;

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


        public HiveMetaDataResultSet build() {

            Schema schema = ThriftUtils.getSchema(thriftSession, operationHandle);

            Iterable<Row> results = ThriftUtils.getResults(thriftSession, operationHandle, Constants.DEFAULT_FETCH_SIZE, schema);

            return new HiveMetaDataResultSet(thriftSession, operationHandle, schema, results.iterator());
        }
    }


}
