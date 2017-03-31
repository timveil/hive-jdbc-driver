package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRowSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveResults implements Closeable {

    // constructor
    private final List<ColumnData> columns;
    private final int maxRows;
    private final int columnCount;
    private final int rowCount;

    private Object[] currentRow = null;

    // should this be atomic?
    private int cursor = 0;

    private HiveResults(List<ColumnData> columns, int maxRows, int columnCount, int rowCount) {

        this.columns = columns;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
        this.maxRows = maxRows;

        this.currentRow = new Object[columnCount];
    }

    private boolean hasNext() {

        if (maxRows > 0 && rowCount >= maxRows) {
            return false;
        }

        return cursor < rowCount;

    }

    public boolean next() {

        if (hasNext()) {
            buildCurrentRow();
            cursor++;
            return true;
        } else {
            return false;
        }


    }

    public int getCursor() {
        return cursor;
    }

    private void buildCurrentRow() {
        for (int i = 0; i < columnCount; i++) {
            currentRow[i] = columns.get(i).getValue(cursor);
        }
    }

    public Object[] getCurrentRow() {
        return currentRow;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public String toString() {
        return "HiveResults{" +
                "maxRows=" + maxRows +
                ", columnCount=" + columnCount +
                ", rowCount=" + rowCount +
                ", currentRow=" + Arrays.toString(currentRow) +
                ", cursor=" + cursor +
                '}';
    }

    public static class Builder {

        private TRowSet rowSet;
        private int maxRows;

        public HiveResults.Builder rowSet(TRowSet rowSet) {
            this.rowSet = rowSet;
            return this;
        }

        public HiveResults.Builder maxRows(int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        public HiveResults build() {
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
                        throw new IllegalStateException("invalid union object");
                    }
                }
            }

            return new HiveResults(columns, maxRows, columns.size(), columns.isEmpty() ? 0 : columns.get(0).getRowCount());
        }
    }

}
