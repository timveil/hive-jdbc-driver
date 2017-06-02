package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class FloatColumnData extends ColumnData<Float> {
    FloatColumnData(ColumnDescriptor descriptor, List<Float> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
