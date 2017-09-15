package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class IntegerColumnData extends AbstractColumnData<Integer> {
    IntegerColumnData(ColumnDescriptor descriptor, List<Integer> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new IntegerColumn(getValue(row));
    }
}
