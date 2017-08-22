package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.bindings.TOperationHandle;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.StopWatch;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.SQLException;
import java.util.Iterator;

public class HiveResultSet extends HiveBaseResultSet {

    private static final Logger log =  LogManager.getLogger(HiveResultSet.class);

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

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

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
        private int maxRows = -1;
        private int fetchSize = -1;
        private int fetchDirection = -1;
        private int resultSetType = -1;
        private int resultSetConcurrency = -1;
        private int resultSetHoldability = -1;

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


            StopWatch sw = new StopWatch();

            sw.start("build schema");

            Schema schema = Schema.builder().session(thriftSession).handle(operationHandle).build();

            sw.stop();

            if (maxRows > 0 && maxRows < fetchSize) {
                fetchSize = maxRows;
            }

            sw.start("get results");
            Iterable<Row> results = ThriftUtils.getResults(thriftSession, operationHandle, fetchSize, schema);
            sw.stop();

            log.debug(sw.prettyPrint());

            if (log.isTraceEnabled()) {
                log.trace("maxRows {}, fetchSize {}, fetchDirection {}, resultSetType {}, resultSetConcurrency {}, resultSetHoldability {}", maxRows, fetchSize, fetchDirection, resultSetType, resultSetConcurrency, resultSetHoldability);
            }

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
