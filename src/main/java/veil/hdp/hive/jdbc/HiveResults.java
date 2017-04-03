package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HiveResults implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(HiveResults.class);

    // constructor
    private final ThriftSession thriftSession;
    private final TOperationHandle operationHandle;
    private final int maxRows;
    private final int fetchSize;

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

    private HiveResults(ThriftSession thriftSession, TOperationHandle operationHandle, int maxRows, int fetchSize) {

        this.thriftSession = thriftSession;
        this.operationHandle = operationHandle;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
    }


    @Override
    public void close() {
        fetchedData.set(null);
        currentRow.set(null);
    }


    public boolean next() throws SQLException {

        // total rows have exceeded max rows
        if (maxRows > 0 && totalRowsIndex.get() >= maxRows) {
            return false;
        }

        // fetch more records if necessary
        if (fetchMore.compareAndSet(true, false)) {

            fetchedRowsIndex.set(0);

            List<ColumnData> columnData = QueryService.fetchResults(thriftSession, operationHandle, TFetchOrientation.FETCH_NEXT, fetchSize);

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


    public static class Builder {

        private ThriftSession thriftSession;
        private TOperationHandle operationHandle;
        private int maxRows = Constants.DEFAULT_MAX_ROWS;
        private int fetchSize = Constants.DEFAULT_FETCH_SIZE;

        public HiveResults.Builder thriftSession(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public HiveResults.Builder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public HiveResults.Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }


        public HiveResults.Builder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public HiveResults build() {
            return new HiveResults(thriftSession, operationHandle, maxRows, fetchSize);
        }


    }

}
