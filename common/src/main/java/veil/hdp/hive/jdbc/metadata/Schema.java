package veil.hdp.hive.jdbc.metadata;

import com.google.common.primitives.Ints;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TTableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private static final Logger log = LoggerFactory.getLogger(Schema.class);

    private final List<ColumnDescriptor> columnDescriptors;

    public Schema(TTableSchema tableSchema) {

        columnDescriptors = new ArrayList<>(tableSchema.getColumnsSize());

        for (TColumnDesc columnDesc : tableSchema.getColumns()) {
            columnDescriptors.add(new ColumnDescriptor(columnDesc));
        }

        columnDescriptors.sort((o1, o2) -> Ints.compare(o1.getPosition(), o2.getPosition()));
    }

    public Schema(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    public List<ColumnDescriptor> getColumns() {
        return columnDescriptors;
    }

    public ColumnDescriptor getColumn(String columnName) {

        for (ColumnDescriptor columnDescriptor : columnDescriptors) {

            if (columnDescriptor.getNormalizedName().equalsIgnoreCase(columnName)) {
                return columnDescriptor;
            }
        }

        return null;
    }

    public ColumnDescriptor getColumn(int position) {
        return columnDescriptors.get(position - 1);
    }

    public void clear() {
        if (columnDescriptors != null && !columnDescriptors.isEmpty()) {

            if (log.isTraceEnabled()) {
                log.trace("clearing columns collection");
            }

            columnDescriptors.clear();
        }
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("\nSchema {\n");

        for (ColumnDescriptor descriptor : columnDescriptors) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", normalizedName: ").append(descriptor.getNormalizedName()).append(", type: ").append(descriptor.getColumnType()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append('}');

        return stringBuilder.toString();
    }
}
