package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class DoubleColumnData extends ColumnData<Double> {
    DoubleColumnData(ColumnDescriptor descriptor, List<Double> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
