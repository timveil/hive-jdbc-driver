package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class BooleanColumnData extends ColumnData<Boolean> {

    BooleanColumnData(ColumnDescriptor descriptor, List<Boolean> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
