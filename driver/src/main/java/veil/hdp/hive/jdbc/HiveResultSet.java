package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.thrift.ThriftOperation;
import veil.hdp.hive.jdbc.utils.FetchIterator;
import veil.hdp.hive.jdbc.utils.ResultSetIterator;
import veil.hdp.hive.jdbc.utils.ResultSetUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class HiveResultSet extends AbstractResultSet {

    private static final Logger log = LogManager.getLogger(HiveResultSet.class);

    private final int maxRows;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;
    // atomic
    private final AtomicBoolean lastColumnNull = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicInteger rowCount = new AtomicInteger(0);
    private final AtomicReference<Row> currentRow = new AtomicReference<>();
    // constructor
    private ThriftOperation thriftOperation;
    private Statement statement;
    private ResultSetIterator iterator;
    // public getter & setter
    private int fetchSize;
    private int fetchDirection;
    private SQLWarning sqlWarning;


    public HiveResultSet(ThriftOperation thriftOperation, Statement statement, ResultSetIterator iterator, int maxRows, int fetchSize, int fetchDirection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.thriftOperation = thriftOperation;
        this.statement = statement;
        this.iterator = iterator;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
        this.fetchDirection = fetchDirection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;

        closed.set(false);
    }

    public static HiveResultSetBuilder builder() {
        return new HiveResultSetBuilder();
    }


    private <T> T checkValue(T value) {
        if (value == null) {
            lastColumnNull.set(true);
        } else {
            lastColumnNull.set(false);
        }

        return value;
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

            try {
                thriftOperation.close();
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            } finally {
                thriftOperation = null;
            }

            iterator = null;

            currentRow.set(null);

        }
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(thriftOperation.getSchema(), columnLabel);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastColumnNull.get();
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return HiveResultSetMetaData.builder().schema(thriftOperation.getSchema()).build();
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
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asBigDecimal());
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
            return checkValue(row.getColumn(columnIndex).asBoolean());
        }

        return false;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asDate());
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
            return checkValue(row.getColumn(columnIndex).asDouble());
        }

        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }


    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asFloat());
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
            return checkValue(row.getColumn(columnIndex).asInt());
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
            return checkValue(row.getColumn(columnIndex).asLong());
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
            return checkValue(row.getColumn(columnIndex).getValue());
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
            return checkValue(row.getColumn(columnIndex).asShort());
        }

        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }


    @Override
    public String getString(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asString());
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
            return checkValue(row.getColumn(columnIndex).asTimestamp());
        }

        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }


    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asByte());
        }

        return 0;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }


    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(checkValue(row.getColumn(columnIndex).asByteArray()));
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
            return checkValue(row.getColumn(columnIndex).asInputStream());
        }

        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }


    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asTime());
        }

        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public boolean next() throws SQLException {
        if (!iterator.hasNext() || (maxRows > 0 && rowCount.get() >= maxRows)) {
            currentRow.set(null);
            return false;
        }

        currentRow.set(iterator.next());

        rowCount.incrementAndGet();

        return true;

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
    public Statement getStatement() throws SQLException {
        return statement;
    }

    public static class HiveResultSetBuilder implements Builder<HiveResultSet> {


        private ThriftOperation thriftOperation;
        private int maxRows = -1;
        private int fetchSize = -1;
        private int fetchDirection = FETCH_FORWARD;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        private int resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        private Statement statement;

        private HiveResultSetBuilder() {
        }

        public HiveResultSetBuilder thriftOperation(ThriftOperation thriftOperation) {
            this.thriftOperation = thriftOperation;
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

        public HiveResultSetBuilder statement(Statement statement) {
            this.statement = statement;
            return this;
        }


        public HiveResultSet build() {

            if (fetchSize < 0) {
                log.warn("fetch size is invalid {}", fetchSize);
            }


            if (maxRows < 0) {
                log.warn("max rows is invalid {}", maxRows);
            }

            if (maxRows > 0 && maxRows < fetchSize) {
                fetchSize = maxRows;
            }

            if (log.isTraceEnabled()) {
                log.trace("maxRows {}, fetchSize {}, fetchDirection {}, resultSetType {}, resultSetConcurrency {}, resultSetHoldability {}", maxRows, fetchSize, fetchDirection, resultSetType, resultSetConcurrency, resultSetHoldability);
            }

            ResultSetIterator iterator = new ResultSetIterator(new FetchIterator(thriftOperation, fetchSize), fetchSize);

            return new HiveResultSet(thriftOperation, statement, iterator,
                    maxRows,
                    fetchSize,
                    fetchDirection,
                    resultSetType,
                    resultSetConcurrency,
                    resultSetHoldability
            );
        }
    }


}
