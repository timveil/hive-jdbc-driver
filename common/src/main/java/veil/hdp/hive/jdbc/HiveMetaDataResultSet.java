package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.QueryUtils;

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

            QueryUtils.closeOperation(session, operationHandle);

            if (schema != null) {
                schema.clear();
            }

            currentRow.set(null);

        }
    }


    public static class Builder {


        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;

        public HiveMetaDataResultSet.Builder thriftSession(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public HiveMetaDataResultSet.Builder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public HiveMetaDataResultSet build() throws SQLException {

            Schema schema = new Schema.Builder().schema(QueryUtils.getResultSetSchema(thriftSession, operationHandle)).build();

            Iterable<Row> results = QueryUtils.getResults(thriftSession, operationHandle, Constants.DEFAULT_FETCH_SIZE, schema);

            return new HiveMetaDataResultSet(thriftSession, operationHandle, schema, results.iterator());
        }
    }


}
