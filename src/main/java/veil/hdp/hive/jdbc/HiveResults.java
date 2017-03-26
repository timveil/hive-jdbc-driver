package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRowSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HiveResults implements Iterable<Object[]> {
    private final List<ColumnData> columns = new ArrayList<>();
    private int rowCount = 0;
    private int columnCount = 0;

    private int cursor = 0;


    public HiveResults(TRowSet tRowSet) {

        if (tRowSet.isSetColumns()) {

            List<TColumn> tColumns = tRowSet.getColumns();

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

        this.columnCount = columns.size();

        this.rowCount = columns.isEmpty() ? 0 : columns.get(0).getRowCount();
    }

    public boolean next() {



    }

    public Object[] getCurrentRow() {

    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {

            private int index;
            private final Object[] row = new Object[columnCount];

            @Override
            public boolean hasNext() {
                return index < rowCount;
            }

            @Override
            public Object[] next() {
                for (int i = 0; i < columnCount; i++) {
                    row[i] = columns.get(i).getValue(index);
                }
                index++;
                return row;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }
}
