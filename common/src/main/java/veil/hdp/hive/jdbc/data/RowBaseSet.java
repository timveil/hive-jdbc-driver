package veil.hdp.hive.jdbc.data;

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

    public static class Builder {
        private ColumnBasedSet columnBasedSet;


        public RowBaseSet.Builder columnBaseSet(ColumnBasedSet columnBasedSet) {
            this.columnBasedSet = columnBasedSet;
            return this;
        }

        public RowBaseSet build() {

            int totalRows = columnBasedSet.getRowCount();

            List<Row> rows = new ArrayList<>(totalRows);

            for (int r = 0; r < totalRows; r++) {
                rows.add(new Row.Builder().columnBasedSet(columnBasedSet).row(r).build());
            }

            return new RowBaseSet(rows);
        }
    }

}
