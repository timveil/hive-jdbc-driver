package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

public class BinaryColumnData extends ColumnData<ByteBuffer> {
    BinaryColumnData(ColumnDescriptor descriptor, List<ByteBuffer> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
