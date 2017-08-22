package veil.hdp.hive.jdbc.metadata;

import com.google.common.primitives.Ints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.TColumnDesc;
import veil.hdp.hive.jdbc.bindings.TOperationHandle;
import veil.hdp.hive.jdbc.bindings.TTableSchema;
import veil.hdp.hive.jdbc.thrift.ThriftSession;
import veil.hdp.hive.jdbc.utils.StopWatch;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.util.*;

public class Schema {

    private static final Logger log =  LogManager.getLogger(Schema.class);

    private final List<ColumnDescriptor> columnDescriptors;
    private final Map<String, ColumnDescriptor> columnMapping;
    private final int columnCount;

    private Schema(Map<String, ColumnDescriptor> columnMapping, List<ColumnDescriptor> columnDescriptors, int columnCount) {
        this.columnMapping = columnMapping;
        this.columnDescriptors = columnDescriptors;
        this.columnCount = columnCount;
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public int getColumnCount() {
        return columnCount;
    }

    public ColumnDescriptor getColumn(String columnName) {
        return columnMapping.get(columnName);
    }

    public ColumnDescriptor getColumn(int position) {
        return columnDescriptors.get(position - 1);
    }

    public void clear() {
        if (columnDescriptors != null && !columnDescriptors.isEmpty()) {
            columnDescriptors.clear();
        }

        if (columnMapping != null && !columnMapping.isEmpty()) {
            columnMapping.clear();
        }
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("\nSchema {\n");

        for (ColumnDescriptor descriptor : columnDescriptors) {
            stringBuilder.append("\tcolumn {").append("name: ").append(descriptor.getName()).append(", label: ").append(descriptor.getLabel()).append(", table name: ").append(descriptor.getTableName()).append(", type: ").append(descriptor.getColumnType()).append(", position: ").append(descriptor.getPosition()).append("}\n");
        }

        stringBuilder.append('}');

        return stringBuilder.toString();
    }

    public static class SchemaBuilder implements Builder<Schema> {

        private static final Comparator<ColumnDescriptor> COLUMN_DESCRIPTOR_COMPARATOR = (o1, o2) -> Ints.compare(o1.getPosition(), o2.getPosition());

        private ThriftSession thriftSession;

        private TOperationHandle operationHandle;

        private List<ColumnDescriptor> columnDescriptors;

        private SchemaBuilder() {
        }

        public SchemaBuilder session(ThriftSession thriftSession) {
            this.thriftSession = thriftSession;
            return this;
        }

        public SchemaBuilder handle(TOperationHandle operationHandle) {
            this.operationHandle = operationHandle;
            return this;
        }


        public SchemaBuilder descriptors(List<ColumnDescriptor> columnDescriptors) {
            this.columnDescriptors = columnDescriptors;
            return this;
        }

        public Schema build() {

            Map<String, ColumnDescriptor> mapping = new HashMap<>();

            StopWatch sw = new StopWatch();

            if (columnDescriptors != null) {
                for (ColumnDescriptor descriptor : columnDescriptors) {
                    mapping.put(descriptor.getName(), descriptor);
                }
            } else {

                sw.start("get schema");
                TTableSchema tableSchema = ThriftUtils.getTableSchema(thriftSession, operationHandle);
                sw.stop();

                sw.start("building mapping");
                for (TColumnDesc columnDesc : tableSchema.getColumns()) {
                    ColumnDescriptor descriptor = ColumnDescriptor.builder().session(thriftSession).thriftColumn(columnDesc).build();
                    mapping.put(descriptor.getName(), descriptor);
                }
                sw.stop();

            }

            List<ColumnDescriptor> columns = new ArrayList<>(mapping.values());

            sw.start("sorting");
            columns.sort(COLUMN_DESCRIPTOR_COMPARATOR);
            sw.stop();

            log.debug(sw.prettyPrint());

            return new Schema(mapping, columns, columns.size());
        }
    }
}
