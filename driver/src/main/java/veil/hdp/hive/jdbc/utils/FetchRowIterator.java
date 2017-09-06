package veil.hdp.hive.jdbc.utils;

import com.google.common.collect.AbstractIterator;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.data.Row;

class FetchRowIterator extends AbstractIterator<Row> {

    private final ColumnBasedSet columnBasedSet;
    private int index = 0;

    public FetchRowIterator(ColumnBasedSet columnBasedSet) {
        this.columnBasedSet = columnBasedSet;
    }

    @Override
    protected Row computeNext() {

        int rowCount = columnBasedSet.getRowCount();

        if (rowCount <= 0) {
            return endOfData();
        }

        if (index < rowCount) {
            Row row = Row.builder().columnBasedSet(columnBasedSet).row(index).build();

            index++;

            return row;
        }

        index = 0;

        return endOfData();
    }
}
