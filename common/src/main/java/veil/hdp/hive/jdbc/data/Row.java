package veil.hdp.hive.jdbc.data;


import com.google.common.primitives.Ints;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveSQLException;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Row {

    private final List<Column> columns;

    private Row(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int position) {
        return columns.get(position - 1);
    }

    public Column getColumn(String name) throws SQLException {


        for (Column column : columns) {
            if (column.getDescriptor().getNormalizedName().equalsIgnoreCase(name)) {
                return column;
            }
        }

        throw new HiveSQLException(MessageFormat.format("invalid column name [{0}] for row;", name));
    }


    public static RowBuilder builder() {
        return new RowBuilder();
    }


    public static class RowBuilder implements Builder<Row> {

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

            columns.sort((o1, o2) -> Ints.compare(o1.getDescriptor().getPosition(), o2.getDescriptor().getPosition()));


            return new Row(columns);

        }
    }
}
