package veil.hdp.hive.jdbc.data;


import com.google.common.primitives.Ints;
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


    public static class Builder {

        private ColumnBasedSet columnBasedSet;
        private int row;


        public Row.Builder columnBasedSet(ColumnBasedSet columnBasedSet) {
            this.columnBasedSet = columnBasedSet;
            return this;
        }

        public Row.Builder row(int row) {
            this.row = row;
            return this;
        }


        public Row build() {


            int columnCount = columnBasedSet.getColumnCount();

            List<Column> columns = new ArrayList<>(columnCount);

            for (ColumnData columnData : columnBasedSet.getColumns()) {
                columns.add(new BaseColumn.Builder().row(row).columnData(columnData).build());
            }

            columns.sort((o1, o2) -> Ints.compare(o1.getDescriptor().getPosition(), o2.getDescriptor().getPosition()));


            return new Row(columns);

        }
    }
}
