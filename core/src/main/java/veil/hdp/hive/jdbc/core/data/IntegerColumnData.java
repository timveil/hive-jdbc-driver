package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class IntegerColumnData extends ColumnData<Integer> {
    IntegerColumnData(ColumnDescriptor descriptor, List<Integer> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
