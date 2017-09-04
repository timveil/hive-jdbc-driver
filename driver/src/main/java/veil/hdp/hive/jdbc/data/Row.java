package veil.hdp.hive.jdbc.data;


import com.google.common.primitives.Ints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.utils.StopWatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Row {

    private static final Logger log = LogManager.getLogger(Row.class);

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

        RowBuilder columnBasedSet(ColumnBasedSet columnBasedSet) {
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

            StopWatch sw = new StopWatch("build row");
            sw.start("loop columns");

            for (ColumnData columnData : columnBasedSet.getColumns()) {
                columns.add(BaseColumn.builder().row(row).columnData(columnData).build());
            }
            sw.stop();

            sw.start("sort columns");

            columns.sort(COLUMN_COMPARATOR);
            sw.stop();

            log.debug(sw.prettyPrint());


            return new Row(columns);

        }
    }
}
