package veil.hdp.hive.jdbc.data;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRowSet;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.SQLException;
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

    public Column getColumn(int position) throws SQLException {

        for (Column column : columns) {
            if (column.getDescriptor().getPosition() == position) {
                return column;
            }
        }

        throw new SQLException("invalid column position [" + position + "] for row; row has [" + columns.size() + "] columns");
    }

    public Column getColumn(String name) throws SQLException {


        for (Column column : columns) {
            if (column.getDescriptor().getNormalizedName().equalsIgnoreCase(name)) {
                return column;
            }
        }

        throw new SQLException("invalid column name [" + name + "] for row;");
    }


    public static class Builder {

        private TRowSet tRowSet;
        private Schema schema;
        private int row;


        public Row.Builder rowSet(TRowSet tRowSet) {
            this.tRowSet = tRowSet;
            return this;
        }

        public Row.Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Row.Builder row(int row) {
            this.row = row;
            return this;
        }


        public Row build() throws SQLException {

            List<Column> columns = new ArrayList<>();

            List<TColumn> tColumns = tRowSet.getColumns();

            for (int c = 0; c < tColumns.size(); c++) {
                TColumn column = tColumns.get(c);

                int position = c + 1;

                ColumnDescriptor descriptor = schema.getColumn(position);

                columns.add(new AbstractColumn.Builder().index(row).column(column).descriptor(descriptor).build());

            }


            return new Row(columns);

        }
    }
}
