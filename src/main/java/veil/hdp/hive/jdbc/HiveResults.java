package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HiveResults implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(HiveResults.class);

    // constructor
    private final TOperationHandle operation;
    private final HiveStatement statement;


    // atomic

    // number of columns in fetched results
    private final AtomicInteger fetchedColumnCount = new AtomicInteger(0);

    // number of rows in first column of fetched result
    private final AtomicInteger fetchedRowCount = new AtomicInteger(0);

    // should i try to fetch more?
    private final AtomicBoolean fetchMore = new AtomicBoolean(true);

    // the fetched results
    private final AtomicReference<List<ColumnData>> fetchedData = new AtomicReference<>();

    // total rows fetched across all fetch attempts
    private final AtomicInteger totalRowsIndex = new AtomicInteger(0);

    // the index of the current fetch
    private final AtomicInteger fetchedRowsIndex = new AtomicInteger(0);

    // the current row
    private final AtomicReference<Object[]> currentRow = new AtomicReference<>();


    private HiveResults(TOperationHandle operation, HiveStatement statement) {
        this.operation = operation;
        this.statement = statement;
    }


    @Override
    public void close() {
        fetchedData.set(null);
        currentRow.set(null);
    }


    public boolean next() throws SQLException {

        // total rows have exceeded max rows
        if (statement.getMaxRows() > 0 && totalRowsIndex.get() >= statement.getMaxRows()) {
            return false;
        }

        // fetch more records if necessary
        if (fetchMore.compareAndSet(true, false)) {

            fetchedRowsIndex.set(0);

            TFetchResultsResp fetchResultsResp = QueryService.fetchResults(statement.getConnection().getThriftSession(), operation, TFetchOrientation.FETCH_NEXT, statement.getFetchSize());

            List<ColumnData> columnData = getColumnData(fetchResultsResp.getResults());

            fetchedData.set(columnData);
            fetchedColumnCount.set(columnData.size());

            ColumnData firstColumn = columnData.get(0);

            if (firstColumn != null) {
                fetchedRowCount.set(firstColumn.getRowCount());
            }

        }

        // if there are no fetched records then return false;
        if (fetchedRowCount.get() == 0) {
            return false;
        }

        // if there are more rows in this fetch block, then set current row and update counters
        if (fetchedRowsIndex.get() < fetchedRowCount.get()) {
            setCurrentRow();
        }

        return true;


    }


    public int getRowIndex() {
        return totalRowsIndex.get();
    }

    public Object[] getCurrentRow() {
        return currentRow.get();
    }

    private void setCurrentRow() {
        int columnCount = fetchedColumnCount.get();

        Object[] row = new Object[columnCount];

        List<ColumnData> columns = fetchedData.get();

        for (int i = 0; i < columnCount; i++) {
            row[i] = columns.get(i).getValue(fetchedRowsIndex.get());
        }

        // set current row
        currentRow.set(row);

        // update total rows
        totalRowsIndex.incrementAndGet();

        // update fetched rows
        int newIndex = fetchedRowsIndex.incrementAndGet();

        // determine if we need to fetch more
        if (newIndex < fetchedRowCount.get()) {
            fetchMore.set(false);
        } else {
            fetchMore.set(true);
        }


    }

    private List<ColumnData> getColumnData(TRowSet rowSet) {
        List<ColumnData> columns = new ArrayList<>();

        if (rowSet != null && rowSet.isSetColumns()) {

            List<TColumn> tColumns = rowSet.getColumns();

            for (TColumn column : tColumns) {

                if (column.isSetBoolVal()) {
                    columns.add(new ColumnData<>(HiveType.BOOLEAN, column.getBoolVal().getValues(), column.getBoolVal().getNulls()));
                } else if (column.isSetByteVal()) {
                    columns.add(new ColumnData<>(HiveType.TINY_INT, column.getByteVal().getValues(), column.getByteVal().getNulls()));
                } else if (column.isSetI16Val()) {
                    columns.add(new ColumnData<>(HiveType.SMALL_INT, column.getI16Val().getValues(), column.getI16Val().getNulls()));
                } else if (column.isSetI32Val()) {
                    columns.add(new ColumnData<>(HiveType.INTEGER, column.getI32Val().getValues(), column.getI32Val().getNulls()));
                } else if (column.isSetI64Val()) {
                    columns.add(new ColumnData<>(HiveType.BIG_INT, column.getI64Val().getValues(), column.getI64Val().getNulls()));
                } else if (column.isSetDoubleVal()) {
                    columns.add(new ColumnData<>(HiveType.DOUBLE, column.getDoubleVal().getValues(), column.getDoubleVal().getNulls()));
                } else if (column.isSetBinaryVal()) {
                    columns.add(new ColumnData<>(HiveType.BINARY, column.getBinaryVal().getValues(), column.getBinaryVal().getNulls()));
                } else if (column.isSetStringVal()) {
                    columns.add(new ColumnData<>(HiveType.STRING, column.getStringVal().getValues(), column.getStringVal().getNulls()));
                } else {
                    throw new IllegalStateException(Utils.format("unknown column type for TColumn [{}]", column));
                }
            }

            tColumns = null;
            rowSet = null;
        }

        return columns;
    }


    public static class Builder {

        private TOperationHandle operation;
        private HiveStatement statement;

        public HiveResults.Builder handle(TOperationHandle operation) {
            this.operation = operation;
            return this;
        }

        public HiveResults.Builder statement(HiveStatement statement) {
            this.statement = statement;
            return this;
        }

        public HiveResults build() {
            return new HiveResults(operation, statement);
        }


    }

}
