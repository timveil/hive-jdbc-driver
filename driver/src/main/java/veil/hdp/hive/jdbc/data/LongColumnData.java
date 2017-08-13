package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

class LongColumnData extends ColumnData<Long> {
    LongColumnData(ColumnDescriptor descriptor, List<Long> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
