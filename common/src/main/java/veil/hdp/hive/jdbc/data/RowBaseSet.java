package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.Builder;

import java.util.ArrayList;
import java.util.List;

public class RowBaseSet {

    private final List<Row> rows;

    private RowBaseSet(List<Row> rows) {
        this.rows = rows;
    }

    public List<Row> getRows() {
        return rows;
    }


    public static RowBasedSetBuilder builder() {
        return new RowBasedSetBuilder();
    }

    public static class RowBasedSetBuilder implements Builder<RowBaseSet> {
        private ColumnBasedSet columnBasedSet;

        private RowBasedSetBuilder() {
        }

        public RowBasedSetBuilder columnBaseSet(ColumnBasedSet columnBasedSet) {
            this.columnBasedSet = columnBasedSet;
            return this;
        }

        public RowBaseSet build() {

            int totalRows = columnBasedSet.getRowCount();

            List<Row> rows = new ArrayList<>(totalRows);

            for (int r = 0; r < totalRows; r++) {
                rows.add(Row.builder().columnBasedSet(columnBasedSet).row(r).build());
            }

            return new RowBaseSet(rows);
        }
    }

}
