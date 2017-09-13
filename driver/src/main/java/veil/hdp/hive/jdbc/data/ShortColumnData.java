package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class ShortColumnData extends AbstractColumnData<Short> {
    ShortColumnData(ColumnDescriptor descriptor, List<Short> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new ShortColumn(getValue(row));
    }
}
