package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

public class VarcharColumn extends StringColumn {

    VarcharColumn(ColumnDescriptor descriptor, String value) {
        super(descriptor, value);
    }
}
