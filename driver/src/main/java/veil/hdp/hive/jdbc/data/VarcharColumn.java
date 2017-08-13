package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

class VarcharColumn extends StringColumn {

    VarcharColumn(ColumnDescriptor descriptor, String value) {
        super(descriptor, value);
    }
}
