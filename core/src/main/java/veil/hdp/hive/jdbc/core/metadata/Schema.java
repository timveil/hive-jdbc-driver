package veil.hdp.hive.jdbc.core.metadata;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.Builder;
import veil.hdp.hive.jdbc.core.thrift.TColumnDesc;
import veil.hdp.hive.jdbc.core.thrift.TTableSchema;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private static final Logger log = LoggerFactory.getLogger(Schema.class);

    private final List<ColumnDescriptor> columnDescriptors;

    private Schema(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
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

    public static class SchemaBuilder implements Builder<Schema> {

        private TTableSchema tableSchema;
        private List<ColumnDescriptor> columnDescriptors;

        private SchemaBuilder() {
        }

        public SchemaBuilder schema(TTableSchema tTableSchema) {
            this.tableSchema = tTableSchema;
            return this;
        }


        public SchemaBuilder descriptors(List<ColumnDescriptor> columnDescriptors) {
            this.columnDescriptors = columnDescriptors;
            return this;
        }

        public Schema build() {

            if (tableSchema != null) {

                columnDescriptors = new ArrayList<>(tableSchema.getColumnsSize());

                for (TColumnDesc columnDesc : tableSchema.getColumns()) {
                    columnDescriptors.add(ColumnDescriptor.builder().thriftColumn(columnDesc).build());
                }
            }

            columnDescriptors.sort((o1, o2) -> Ints.compare(o1.getPosition(), o2.getPosition()));

            return new Schema(columnDescriptors);
        }
    }
}
