package veil.hdp.hive.jdbc.metadata;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.thrift.TTableSchema;

public class TableSchema extends org.apache.hive.service.cli.TableSchema {

    public TableSchema(TTableSchema tTableSchema) {
        super(tTableSchema);
    }

    public ColumnDescriptor getColumnDescriptorForName(String columnName) {

        for (ColumnDescriptor columnDescriptor : getColumnDescriptors()) {

            String normalizedName = getNormalizedName(columnDescriptor);

            if (normalizedName.equalsIgnoreCase(columnName)) {
                return columnDescriptor;
            }
        }

        return null;
    }

    public String getNormalizedName(ColumnDescriptor columnDescriptor) {
        String name = columnDescriptor.getName();

        if (name.contains(".")) {
            name = name.substring(name.lastIndexOf(".") + 1);
        }

        return name.toLowerCase();
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("\nTableSchema {\n");

        for (ColumnDescriptor descriptor : getColumnDescriptors()) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", type: ").append(descriptor.getType()).append(", position: ").append(descriptor.getOrdinalPosition()).append("}\n");
        }

        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
