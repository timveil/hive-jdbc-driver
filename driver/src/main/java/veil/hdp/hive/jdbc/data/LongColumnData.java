package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class LongColumnData extends AbstractColumnData<Long> {
    LongColumnData(ColumnDescriptor descriptor, List<Long> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new LongColumn(getValue(row));
    }
}
