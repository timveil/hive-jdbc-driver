package veil.hdp.hive.jdbc.data;


import com.google.common.primitives.Ints;
import veil.hdp.hive.jdbc.Builder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Row {

    private final List<Column> columns;

    private Row(List<Column> columns) {
        this.columns = columns;
    }

    public static RowBuilder builder() {
        return new RowBuilder();
    }

    public Column getColumn(int position) {
        return columns.get(position - 1);
    }

    public static class RowBuilder implements Builder<Row> {

        private static final Comparator<Column> COLUMN_COMPARATOR = (o1, o2) -> Ints.compare(o1.getDescriptor().getPosition(), o2.getDescriptor().getPosition());

        private ColumnBasedSet columnBasedSet;
        private int row;

        private RowBuilder() {
        }

        public RowBuilder columnBasedSet(ColumnBasedSet columnBasedSet) {
            this.columnBasedSet = columnBasedSet;
            return this;
        }

        public RowBuilder row(int row) {
            this.row = row;
            return this;
        }


        public Row build() {


            int columnCount = columnBasedSet.getColumnCount();

            List<Column> columns = new ArrayList<>(columnCount);

            for (ColumnData columnData : columnBasedSet.getColumns()) {
                columns.add(BaseColumn.builder().row(row).columnData(columnData).build());
            }

            columns.sort(COLUMN_COMPARATOR);


            return new Row(columns);

        }
    }
}
