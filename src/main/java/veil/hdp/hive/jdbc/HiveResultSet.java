package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.column.Row;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HiveResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private final Schema schema;
    private final Iterator<Row> results;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicBoolean lastColumnNull = new AtomicBoolean(true);
    private final AtomicInteger rowCount = new AtomicInteger(0);
    private final AtomicReference<Row> currentRow = new AtomicReference<>();

    // public getter & setter
    private int fetchSize;
    private int maxRows;
    private int fetchDirection;
    private SQLWarning sqlWarning = null;


    private HiveResultSet(Schema schema, int maxRows, int fetchSize, int fetchDirection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, Iterator<Row> results) {
        this.fetchDirection = fetchDirection;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

        this.schema = schema;
        this.results = results;

        closed.set(false);
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
    public boolean isClosed() {
        return closed.get();
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
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(schema, columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asBigDecimal();
        }

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asBoolean();
        }

        return false;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public int getConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asDate();
        }

        return null;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asDouble();
        }

        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
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
    public float getFloat(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asFloat();
        }

        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asInt();
        }

        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asLong();
        }

        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asObject();
        }

        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asShort();
        }

        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public HiveStatement getStatement() throws SQLException {
        // todo; don't love
        return null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asString();
        }

        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asTimestamp();
        }

        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData.Builder().schema(schema).build();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asByte();
        }

        return 0;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastColumnNull.get();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asByteArray();
        }

        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asInputStream();
        }

        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return row.getColumn(columnIndex).asTime();
        }

        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public String toString() {
        return "HiveResultSet{" +
                ", currentSchema=" + schema +
                ", fetchSize=" + fetchSize +
                ", fetchDirection=" + fetchDirection +
                ", resultSetType=" + resultSetType +
                ", resultSetConcurrency=" + resultSetConcurrency +
                ", resultSetHoldability=" + resultSetHoldability +
                ", sqlWarning=" + sqlWarning +
                ", lastColumnNull=" + lastColumnNull +
                '}';
    }


    public static class Builder {


        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;
        private int maxRows = Constants.DEFAULT_MAX_ROWS;
        private int fetchSize = Constants.DEFAULT_FETCH_SIZE;
        private int fetchDirection = ResultSet.FETCH_FORWARD;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        private int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;


        public HiveResultSet.Builder thriftSession(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public HiveResultSet.Builder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }

        public HiveResultSet.Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }


        public HiveResultSet.Builder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public HiveResultSet.Builder fetchDirection(int fetchDirection) {
            this.fetchDirection = fetchDirection;
            return this;
        }


        public HiveResultSet.Builder resultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }


        public HiveResultSet.Builder resultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }


        public HiveResultSet.Builder resultSetHoldability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }


        public HiveResultSet build() throws SQLException {

            Schema schema = new Schema(QueryService.getResultSetSchema(thriftSession, operationHandle));

            if (maxRows > 0 && maxRows < fetchSize) {
                fetchSize = maxRows;
            }

            Iterable<Row> results = QueryService.getResults(thriftSession, operationHandle, fetchSize, schema);

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
