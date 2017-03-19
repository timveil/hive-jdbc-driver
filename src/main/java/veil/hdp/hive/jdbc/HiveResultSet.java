package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;

public class HiveResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private final HiveConnection connection;
    private final HiveStatement statement;
    private final TOperationHandle statementHandle;
    private final TableSchema tableSchema;

    // private
    private RowSet rowSet;
    private Iterator<Object[]> rowSetIterator;
    private Object[] row;

    // public getter & setter
    private int fetchSize;
    private int fetchDirection;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;

    // public getter only
    private int rowCount;
    private boolean closed;


    HiveResultSet(HiveConnection connection, HiveStatement statement, TOperationHandle statementHandle, TableSchema tableSchema) throws SQLException {
        this.connection = connection;
        this.statement = statement;
        this.tableSchema = tableSchema;
        this.statementHandle = statementHandle;
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.resultSetType = statement.getResultSetType();
        this.resultSetConcurrency = statement.getResultSetConcurrency();
        this.resultSetHoldability = statement.getResultSetHoldability();
    }

    @Override
    public boolean next() throws SQLException {

        if (statement.getMaxRows() > 0 && rowCount >= statement.getMaxRows()) {
            return false;
        }

        try {

            if (rowSet == null || !rowSetIterator.hasNext()) {
                TRowSet results = HiveServiceUtils.fetchResults(connection.getClient(), statementHandle, TFetchOrientation.FETCH_NEXT, fetchSize);
                rowSet = RowSetFactory.create(results, connection.getProtocolVersion());
                rowSetIterator = rowSet.iterator();
            }

            if (rowSetIterator.hasNext()) {
                row = rowSetIterator.next();
            } else {
                return false;
            }

            rowCount++;

        } catch (TException e) {
            throw new SQLException(e.getMessage(), e);
        }

        return true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws SQLException {

        if (!closed) {

            if (log.isDebugEnabled()) {
                log.debug("attempting to close {}", this.getClass().getName());
            }

            rowSet = null;
            rowSetIterator = null;
            row = null;

            closed = true;
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
        return rowCount == 0;
    }

    @Override
    public int getRow() throws SQLException {
        return rowCount;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(tableSchema, columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.DECIMAL_TYPE);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.BOOLEAN_TYPE);
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
        return (Date) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.DATE_TYPE);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return (Double) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.DOUBLE_TYPE);
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
        return (Float) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.FLOAT_TYPE);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return (Integer) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.INT_TYPE);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return (Long) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.BIGINT_TYPE);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (Short) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.SMALLINT_TYPE);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return (String) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.STRING_TYPE);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.TIMESTAMP_TYPE);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData(tableSchema);
    }


    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (Byte)ResultSetUtils.getColumnValue(tableSchema, row, columnIndex, Type.TINYINT_TYPE);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSetHoldability;
    }

    /*

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return super.getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return super.getBinaryStream(columnLabel);
    }
    */
}
