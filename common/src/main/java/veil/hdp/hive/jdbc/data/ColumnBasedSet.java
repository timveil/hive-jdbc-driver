package veil.hdp.hive.jdbc.data;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRowSet;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.util.ArrayList;
import java.util.List;

public class ColumnBasedSet {

    private final List<ColumnData> columns;

    private ColumnBasedSet(List<ColumnData> columns) {
        this.columns = columns;
    }

    public List<ColumnData> getColumns() {
        return columns;
    }

    public int getRowCount() {
        return columns.get(0).getRowCount();
    }

    public int getColumnCount() {
        return columns.size();
    }

    public static class Builder {

        private TRowSet rowSet;
        private Schema schema;


        public ColumnBasedSet.Builder rowSet(TRowSet tRowSet) {
            this.rowSet = tRowSet;
            return this;
        }

        public ColumnBasedSet.Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public ColumnBasedSet build() {

            List<ColumnData> columnData = null;

            if (rowSet != null && rowSet.isSetColumns()) {

                List<TColumn> tColumns = rowSet.getColumns();

                columnData = new ArrayList<>(tColumns.size());

                int position = 1;

                for (TColumn column : tColumns) {

                    columnData.add(new ColumnData.Builder().column(column).descriptor(schema.getColumn(position)).build());

                    position++;
                }
            }

            return new ColumnBasedSet(columnData);
        }
    }

}
