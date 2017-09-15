package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

class BinaryColumnData extends AbstractColumnData<ByteBuffer> {
    BinaryColumnData(ColumnDescriptor descriptor, List<ByteBuffer> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {
        return new BinaryColumn(getValue(row));
    }
}
