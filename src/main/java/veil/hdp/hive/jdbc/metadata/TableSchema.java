package veil.hdp.hive.jdbc.metadata;

import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TTableSchema;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    private final List<ColumnDescriptor> columns = new ArrayList<>();

    public TableSchema(TTableSchema tTableSchema) {
        for (TColumnDesc tColumnDesc : tTableSchema.getColumns()) {
            columns.add(new ColumnDescriptor(tColumnDesc));
        }
    }

    public List<ColumnDescriptor> getColumns() {
        return columns;
    }

    public ColumnDescriptor getColumn(String columnName) {

        for (ColumnDescriptor columnDescriptor : getColumns()) {

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

        for (ColumnDescriptor descriptor : getColumns()) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", type: ").append(descriptor.getType()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
