package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.TColumn;
import veil.hdp.hive.jdbc.bindings.TRowSet;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.util.ArrayList;
import java.util.List;

public class ColumnBasedSet {

    private final int rowCount;
    private final List<ColumnData> columns;

    private ColumnBasedSet(int rowCount, List<ColumnData> columns) {
        this.rowCount = rowCount;
        this.columns = columns;
    }

    public static ColumnBasedSetBuilder builder() {
        return new ColumnBasedSetBuilder();
    }

    public List<ColumnData> getColumns() {
        return columns;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public static class ColumnBasedSetBuilder implements Builder<ColumnBasedSet> {

        private TRowSet rowSet;
        private Schema schema;

        private ColumnBasedSetBuilder() {
        }

        public ColumnBasedSetBuilder rowSet(TRowSet tRowSet) {
            this.rowSet = tRowSet;
            return this;
        }

        public ColumnBasedSetBuilder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public ColumnBasedSet build() {

            List<TColumn> tColumns = rowSet.getColumns();

            List<ColumnData> columns = new ArrayList<>(tColumns.size());

            int position = 1;

            int rowCount = -1;

            for (TColumn column : tColumns) {

                ColumnData data = ColumnData.builder().column(column).descriptor(schema.getColumn(position)).build();

                if (rowCount == -1) {
                    rowCount = data.getRowCount();
                }

                columns.add(data);

                position++;
            }


            return new ColumnBasedSet(rowCount, columns);
        }
    }

}
