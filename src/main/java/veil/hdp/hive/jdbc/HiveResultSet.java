package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.metadata.TableSchema;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;
import veil.hdp.hive.jdbc.utils.ResultSetUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.util.Iterator;

public class HiveResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private TCLIService.Client client;
    private TOperationHandle statementHandle;
    private TProtocolVersion protocol;
    private HiveStatement statement;
    private boolean scrollable;
    private int maxRows;

    // private
    private TableSchema tableSchema;
    private boolean fetchFirst = false;
    private RowSet fetchedRows;
    private Iterator<Object[]> fetchedRowsItr;
    private Object[] row;


    // public getter & setter
    private int rowsFetched;
    private int fetchSize;


    // lets get rid of this
    private boolean emptyResultSet;


    public HiveResultSet(TCLIService.Client client, TOperationHandle statementHandle, TProtocolVersion protocol, HiveStatement statement, boolean scrollable, int maxRows) throws TException {
        this.client = client;
        this.statementHandle = statementHandle;
        this.protocol = protocol;
        this.statement = statement;
        this.scrollable = scrollable;
        this.maxRows = maxRows;

        this.tableSchema = new TableSchema(HiveServiceUtils.getSchema(client, statementHandle));

        if (log.isDebugEnabled()) {
            log.debug(tableSchema.toString());
        }

        // parse schema for columnNames, etc
    }


    @Override
    public boolean next() throws SQLException {

        // i bet i can improve this eventually

        if (emptyResultSet || (maxRows > 0 && rowsFetched >= maxRows)) {
            return false;
        }

        try {
            TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT;

            if (fetchFirst) {
                orientation = TFetchOrientation.FETCH_FIRST;
                fetchedRows = null;
                fetchedRowsItr = null;
                fetchFirst = false;
            }

            if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
                TRowSet results = HiveServiceUtils.fetchResults(client, statementHandle, orientation, fetchSize);
                fetchedRows = RowSetFactory.create(results, protocol);
                fetchedRowsItr = fetchedRows.iterator();
            }

            if (fetchedRowsItr.hasNext()) {
                row = fetchedRowsItr.next();
            } else {
                return false;
            }

            rowsFetched++;


        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }

        return true;
    }

    @Override
    public void close() throws SQLException {
        client = null;
        statementHandle = null;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getType() throws SQLException {
        if (scrollable) {
            return ResultSet.TYPE_SCROLL_INSENSITIVE;
        } else {
            return ResultSet.TYPE_FORWARD_ONLY;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void beforeFirst() throws SQLException {

        if (!scrollable) {
            throw new SQLException("ResultSet not SCROLLABLE");
        }

        this.fetchFirst = true;
        this.rowsFetched = 0;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowsFetched == 0;
    }

    @Override
    public int getRow() throws SQLException {
        return rowsFetched;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(tableSchema, columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        MathContext mc = new MathContext(scale);
        return getBigDecimal(columnIndex).round(mc);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (Boolean) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return (Double)ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (Float)ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return (Integer) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return (Long) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
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
        return (Short) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
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
        return (String) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) ResultSetUtils.getColumnValue(tableSchema, row, columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

       /*

       @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData(columnNames, columnTypes, columnAttributes);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return super.getByte(columnIndex);
    }



    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return super.getByte(columnIndex);
    }

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
