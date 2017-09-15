package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class BooleanColumnData extends AbstractColumnData<Boolean> {

    BooleanColumnData(ColumnDescriptor descriptor, List<Boolean> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new BooleanColumn(getValue(row));
    }
}
