package veil.hdp.hive.jdbc;

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

            if (columnDescriptor.getNormalizedName().equalsIgnoreCase(columnName)) {
                return columnDescriptor;
            }
        }

        return null;
    }

    public ColumnDescriptor getColumn(int position) {

        for (ColumnDescriptor columnDescriptor : getColumns()) {
            if (position == columnDescriptor.getPosition()) {
                return columnDescriptor;
            }
        }

        return null;
    }



    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("\nTableSchema {\n");

        for (ColumnDescriptor descriptor : getColumns()) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", normalizedName: ").append(descriptor.getNormalizedName()).append(", type: ").append(descriptor.getType()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
