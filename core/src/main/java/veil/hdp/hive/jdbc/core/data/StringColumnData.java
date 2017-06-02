package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public class StringColumnData extends ColumnData<String> {

    StringColumnData(ColumnDescriptor descriptor, List<String> values, BitSet nulls) {
        super(descriptor, values, nulls);
    }
}
