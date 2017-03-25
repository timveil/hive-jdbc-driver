package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TTableSchema;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private final List<Column> columns;

    public Schema(TTableSchema tableSchema) {

        columns = new ArrayList<>();

        for (TColumnDesc columnDesc : tableSchema.getColumns()) {
            columns.add(new Column(columnDesc));
        }
    }

    public Schema(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(String columnName) {

        for (Column column : getColumns()) {

            if (column.getNormalizedName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }

        return null;
    }

    public Column getColumn(int position) {

        for (Column column : getColumns()) {
            if (position == column.getPosition()) {
                return column;
            }
        }

        return null;
    }


    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("\nSchema {\n");

        for (Column descriptor : getColumns()) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", normalizedName: ").append(descriptor.getNormalizedName()).append(", type: ").append(descriptor.getColumnType()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
