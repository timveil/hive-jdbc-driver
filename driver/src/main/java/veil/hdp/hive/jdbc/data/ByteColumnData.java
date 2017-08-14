package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class ByteColumnData extends ColumnData<Byte> {
    ByteColumnData(ColumnDescriptor descriptor, List<Byte> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
