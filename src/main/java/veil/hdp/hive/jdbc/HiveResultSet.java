package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private final HiveStatement statement;
    private final Schema schema;
    private final HiveResults hiveResults;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicBoolean lastColumnNull = new AtomicBoolean(true);

    // public getter & setter
    private int fetchSize;
    private int fetchDirection;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;
    private SQLWarning sqlWarning = null;


    private HiveResultSet(HiveStatement statement, Schema schema, HiveResults hiveResults) throws SQLException {
        this.statement = statement;
        this.schema = schema;
        this.hiveResults = hiveResults;
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.resultSetType = statement.getResultSetType();
        this.resultSetConcurrency = statement.getResultSetConcurrency();
        this.resultSetHoldability = statement.getResultSetHoldability();
    }

    @Override
    public boolean next() throws SQLException {
        return hiveResults.next();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {

            if (log.isDebugEnabled()) {
                log.debug("attempting to close {}", this.getClass().getName());
            }

            hiveResults.close();

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
        return hiveResults.getRowIndex();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(schema, columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) getColumnValue(columnIndex, HiveType.DECIMAL);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) getColumnValue(columnIndex, HiveType.BOOLEAN);
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
        return (Date) getColumnValue(columnIndex, HiveType.DATE);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return (Double) getColumnValue(columnIndex, HiveType.DOUBLE);
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
        return (Float) getColumnValue(columnIndex, HiveType.FLOAT);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return (Integer) getColumnValue(columnIndex, HiveType.INTEGER);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return (Long) getColumnValue(columnIndex, HiveType.BIG_INT);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getColumnValue(columnIndex, null);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (Short) getColumnValue(columnIndex, HiveType.SMALL_INT);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public HiveStatement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return (String) getColumnValue(columnIndex, HiveType.STRING);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getColumnValue(columnIndex, HiveType.TIMESTAMP);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData(schema);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (Byte) getColumnValue(columnIndex, HiveType.TINY_INT);
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
        return (byte[]) getColumnValue(columnIndex, HiveType.BINARY);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getValueAsStream(columnIndex);
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
        return getValueAsTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    private Object getColumnValue(int columnIndex, HiveType targetType) throws SQLException {

        Object columnValue = ResultSetUtils.getColumnValue(schema, hiveResults.getCurrentRow(), columnIndex, targetType);

        lastColumnNull.set(columnValue == null);

        return columnValue;
    }

    private InputStream getValueAsStream(int columnIndex) throws SQLException {

        InputStream columnValue = ResultSetUtils.getColumnValueAsStream(schema, hiveResults.getCurrentRow(), columnIndex);

        lastColumnNull.set(columnValue == null);

        return columnValue;
    }

    private Time getValueAsTime(int columnIndex) throws SQLException {

        Time columnValue = ResultSetUtils.getColumnValueAsTime(schema, hiveResults.getCurrentRow(), columnIndex);

        lastColumnNull.set(columnValue == null);

        return columnValue;
    }

    @Override
    public String toString() {
        return "HiveResultSet{" +
                "statement=" + statement +
                ", schema=" + schema +
                ", hiveResults=" + hiveResults +
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

        private ThriftOperation thriftOperation;
        private TOperationHandle handle;
        private HiveStatement statement;
        private Schema schema;

        public HiveResultSet.Builder operation(ThriftOperation thriftOperation) {
            this.thriftOperation = thriftOperation;
            return this;
        }

        public HiveResultSet.Builder handle(TOperationHandle handle) {
            this.handle = handle;
            return this;
        }

        public HiveResultSet.Builder statement(HiveStatement statement) {
            this.statement = statement;
            return this;
        }

        public HiveResultSet.Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public HiveResultSet build() throws SQLException {

            if (schema == null) {
                schema = new Schema(HiveServiceUtils.getResultSetSchema(thriftOperation.getClient(), thriftOperation.getOperationHandle()));
            }

            TOperationHandle oh;

            if (thriftOperation != null) {
                oh = thriftOperation.getOperationHandle();
            } else {
                oh = handle;
            }

            return new HiveResultSet(statement, schema, new HiveResults.Builder().handle(oh).statement(statement).build());
        }
    }


}
