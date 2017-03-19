package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TTableSchema;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    private final List<ColumnDescriptor> columns;

    public TableSchema(TTableSchema tTableSchema) {

        columns = new ArrayList<>();

        for (TColumnDesc tColumnDesc : tTableSchema.getColumns()) {
            columns.add(new ColumnDescriptor(tColumnDesc));
        }
    }

    public TableSchema(List<ColumnDescriptor> columns) {
        this.columns = columns;
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
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", normalizedName: ").append(descriptor.getNormalizedName()).append(", type: ").append(descriptor.getTypeDescriptor()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
