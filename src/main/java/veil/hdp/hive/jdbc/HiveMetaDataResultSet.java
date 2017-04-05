package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.QueryUtils;
import veil.hdp.hive.jdbc.utils.ResultSetUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class HiveMetaDataResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveMetaDataResultSet.class);

    // constructor
    private final ThriftSession session;
    private final Schema schema;
    private final Iterator<Row> results;
    private final TOperationHandle operationHandle;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicBoolean lastColumnNull = new AtomicBoolean(true);
    private final AtomicReference<Row> currentRow = new AtomicReference<>();

    // public getter & setter
    private SQLWarning sqlWarning = null;


    private HiveMetaDataResultSet(ThriftSession session, TOperationHandle operationHandle, Schema schema, Iterator<Row> results) {
        this.session = session;
        this.schema = schema;
        this.results = results;
        this.operationHandle = operationHandle;

        closed.set(false);
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
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            QueryUtils.closeOperation(session, operationHandle);

            if (schema != null) {
                schema.clear();
            }

            currentRow.set(null);

        }
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(schema, columnLabel);
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

            Schema schema = new Schema(QueryUtils.getResultSetSchema(thriftSession, operationHandle));

            Iterable<Row> results = QueryUtils.getResults(thriftSession, operationHandle, Constants.DEFAULT_FETCH_SIZE, schema);

            return new HiveMetaDataResultSet(thriftSession, operationHandle, schema, results.iterator());
        }
    }


}
