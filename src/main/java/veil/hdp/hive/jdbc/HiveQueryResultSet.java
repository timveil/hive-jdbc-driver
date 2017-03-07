package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.metadata.TableSchema;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.*;
import java.util.Iterator;
import java.util.List;

public class HiveQueryResultSet extends HiveResultSet {

    // constructor
    private TCLIService.Client client;
    private TOperationHandle statementHandle;
    private TProtocolVersion protocol;
    private boolean scrollable;
    private int maxRows;

    // private
    private TableSchema tableSchema;

    // public getter & setter




    // lets get rid of this
    private boolean emptyResultSet;
    private int rowsFetched;
    private boolean fetchFirst = false;
    private RowSet fetchedRows;
    private Iterator<Object[]> fetchedRowsItr;
    private int fetchSize;
    private Object[] row;
    private HiveStatement statement;




    public HiveQueryResultSet(TCLIService.Client client, TOperationHandle statementHandle,TProtocolVersion protocol, boolean scrollable, int maxRows) throws TException {
        this.client = client;
        this.statementHandle = statementHandle;
        this.scrollable = scrollable;
        this.maxRows = maxRows;
        this.protocol = protocol;

        this.tableSchema = new TableSchema(HiveServiceUtils.getSchema(client, statementHandle));

        // parse schema for columnNames, etc
    }



    @Override
    public boolean next() throws SQLException {

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

  /*  @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData(columnNames, columnTypes, columnAttributes);
    }*/

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

        ColumnDescriptor columnDescriptorForName = tableSchema.getColumnDescriptorForName(columnLabel);

        if (columnDescriptorForName != null) {
            return columnDescriptorForName.getOrdinalPosition();
        }

        throw new SQLException("Could not find column for name " + columnLabel + " in TableSchema " + tableSchema);
    }
}
