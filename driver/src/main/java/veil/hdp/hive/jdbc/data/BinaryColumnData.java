package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

class BinaryColumnData extends ColumnData<ByteBuffer> {
    BinaryColumnData(ColumnDescriptor descriptor, List<ByteBuffer> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
