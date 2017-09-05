package veil.hdp.hive.jdbc.data;

import com.google.common.collect.AbstractIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.TColumn;
import veil.hdp.hive.jdbc.bindings.TRowSet;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ColumnBasedSet implements Iterable<Row> {

    private static final Logger log = LogManager.getLogger(ColumnBasedSet.class);

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

    @Override
    public Iterator<Row> iterator() {

        return new AbstractIterator<Row>() {

            private int index = 0;

            @Override
            protected Row computeNext() {

                if (rowCount <= 0) {
                    return endOfData();
                }

                if (index < rowCount) {
                    Row row = Row.builder().columnBasedSet(ColumnBasedSet.this).row(index).build();

                    index++;

                    return row;
                }

                index = 0;

                return endOfData();
            }
        };
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

            if (!tColumns.isEmpty()) {

                for (TColumn column : tColumns) {

                    ColumnData data = ColumnData.builder().column(column).descriptor(schema.getColumn(position)).build();

                    if (rowCount == -1) {
                        rowCount = data.getRowCount();
                    }

                    columns.add(data);

                    position++;
                }
            }


            return new ColumnBasedSet(rowCount, columns);
        }
    }

}
