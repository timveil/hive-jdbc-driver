package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.TOperationHandle;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class HiveResultSet extends HiveBaseResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private final int maxRows;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;
    private final Iterator<Row> results;


    // public getter & setter
    private int fetchSize;
    private int fetchDirection;


    private HiveResultSet(Schema schema, int maxRows, int fetchSize, int fetchDirection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, Iterator<Row> results) {

        super(schema);

        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
        this.fetchDirection = fetchDirection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.results = results;

    }

    public static HiveResultSetBuilder builder() {
        return new HiveResultSetBuilder();
    }

    @Override
    public boolean next() throws SQLException {
        if (!results.hasNext() || (maxRows > 0 && rowCount.get() >= maxRows)) {
            currentRow.set(null);
            return false;
        }

        currentRow.set(results.next());

        rowCount.incrementAndGet();

        return true;

    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {

            if (schema != null) {
                schema.clear();
            }

            currentRow.set(null);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetType;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return getRow() == 0;
    }

    @Override
    public int getRow() throws SQLException {
        return rowCount.get();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public HiveStatement getStatement() throws SQLException {
        // todo; don't love
        return null;
    }

    public static class HiveResultSetBuilder implements Builder<HiveResultSet> {


        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;
        private int maxRows = Constants.DEFAULT_MAX_ROWS;
        private int fetchSize = Constants.DEFAULT_FETCH_SIZE;
        private int fetchDirection = ResultSet.FETCH_FORWARD;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        private int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        private HiveResultSetBuilder() {
        }

        public HiveResultSetBuilder thriftSession(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public HiveResultSetBuilder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }

        public HiveResultSetBuilder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }


        public HiveResultSetBuilder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public HiveResultSetBuilder fetchDirection(int fetchDirection) {
            this.fetchDirection = fetchDirection;
            return this;
        }


        public HiveResultSetBuilder resultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }


        public HiveResultSetBuilder resultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }


        public HiveResultSetBuilder resultSetHoldability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }


        public HiveResultSet build() {


            Schema schema = ThriftUtils.getSchema(thriftSession, operationHandle);

            if (maxRows > 0 && maxRows < fetchSize) {
                fetchSize = maxRows;
            }

            Iterable<Row> results = ThriftUtils.getResults(thriftSession, operationHandle, fetchSize, schema);

            return new HiveResultSet(schema,
                    maxRows,
                    fetchSize,
                    fetchDirection,
                    resultSetType,
                    resultSetConcurrency,
                    resultSetHoldability,
                    results.iterator());
        }
    }


}
