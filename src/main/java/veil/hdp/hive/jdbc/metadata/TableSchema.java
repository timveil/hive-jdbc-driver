package veil.hdp.hive.jdbc.metadata;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.thrift.TTableSchema;

public class TableSchema extends org.apache.hive.service.cli.TableSchema {

    public TableSchema(TTableSchema tTableSchema) {
        super(tTableSchema);
    }

    public ColumnDescriptor getColumnDescriptorForName(String columnName) {


        for (ColumnDescriptor columnDescriptor : getColumnDescriptors()) {

            if (columnDescriptor.getName().equalsIgnoreCase(columnName)) {
                return columnDescriptor;
            }
        }

        return null;
    }

}
