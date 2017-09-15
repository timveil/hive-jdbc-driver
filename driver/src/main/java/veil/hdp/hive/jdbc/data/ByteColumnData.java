package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class ByteColumnData extends AbstractColumnData<Byte> {
    ByteColumnData(ColumnDescriptor descriptor, List<Byte> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new ByteColumn(getValue(row));
    }
}
