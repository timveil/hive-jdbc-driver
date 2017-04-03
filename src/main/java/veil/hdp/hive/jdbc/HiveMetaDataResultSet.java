package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveMetaDataResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveMetaDataResultSet.class);

    // constructor
    private final ThriftSession session;
    private final Schema schema;
    private final HiveResults hiveResults;
    private final TOperationHandle operationHandle;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicBoolean lastColumnNull = new AtomicBoolean(true);

    // public getter & setter
    private SQLWarning sqlWarning = null;


    private HiveMetaDataResultSet(ThriftSession session, TOperationHandle operationHandle, Schema schema, HiveResults hiveResults) throws SQLException {
        this.session = session;
        this.schema = schema;
        this.hiveResults = hiveResults;
        this.operationHandle = operationHandle;

        closed.set(false);
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

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            QueryService.closeOperation(session, operationHandle);

            if (hiveResults != null) {
                hiveResults.close();
            }

            if (schema != null) {
                schema.clear();
            }


        }
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(schema, columnLabel);
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
    public double getDouble(int columnIndex) throws SQLException {
        return (Double) getColumnValue(columnIndex, HiveType.DOUBLE);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
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
    public short getShort(int columnIndex) throws SQLException {
        return (Short) getColumnValue(columnIndex, HiveType.SMALL_INT);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
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
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData.Builder().schema(schema).build();
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
    public boolean wasNull() throws SQLException {
        return lastColumnNull.get();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }


    private Object getColumnValue(int columnIndex, HiveType targetType) throws SQLException {

        Object columnValue = ResultSetUtils.getColumnValue(schema, hiveResults.getCurrentRow(), columnIndex, targetType);

        lastColumnNull.set(columnValue == null);

        return columnValue;
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

            Schema schema = new Schema(QueryService.getResultSetSchema(thriftSession, operationHandle));

            HiveResults hiveResults = new HiveResults.Builder()
                    .thriftSession(thriftSession)
                    .handle(operationHandle)
                    .build();

            return new HiveMetaDataResultSet(thriftSession, operationHandle, schema, hiveResults);
        }
    }


}
