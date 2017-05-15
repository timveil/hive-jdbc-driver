package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class ShortColumnData extends ColumnData<Short> {
    ShortColumnData(ColumnDescriptor descriptor, List<Short> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
