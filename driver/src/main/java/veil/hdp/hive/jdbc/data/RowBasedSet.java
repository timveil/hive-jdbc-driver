package veil.hdp.hive.jdbc.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.utils.StopWatch;

import java.util.ArrayList;
import java.util.List;

public class RowBasedSet {

    private static final Logger log = LogManager.getLogger(RowBasedSet.class);

    private final List<Row> rows;

    private RowBasedSet(List<Row> rows) {
        this.rows = rows;
    }

    public static RowBasedSetBuilder builder() {
        return new RowBasedSetBuilder();
    }

    public List<Row> getRows() {
        return rows;
    }

    public static class RowBasedSetBuilder implements Builder<RowBasedSet> {
        private ColumnBasedSet columnBasedSet;

        private RowBasedSetBuilder() {
        }

        public RowBasedSetBuilder columnBaseSet(ColumnBasedSet columnBasedSet) {
            this.columnBasedSet = columnBasedSet;
            return this;
        }

        public RowBasedSet build() {

            int totalRows = columnBasedSet.getRowCount();

            List<Row> rows = new ArrayList<>(totalRows);

            StopWatch sw = new StopWatch("build RowBasedSet");
            sw.start();
            for (int r = 0; r < totalRows; r++) {
                rows.add(Row.builder().columnBasedSet(columnBasedSet).row(r).build());
            }
            sw.stop();

            log.debug(sw.prettyPrint());

            return new RowBasedSet(rows);
        }
    }

}
