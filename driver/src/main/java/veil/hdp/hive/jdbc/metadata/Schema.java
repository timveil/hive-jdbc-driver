/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.util.*;

public class Schema {

    private static final Logger log = LogManager.getLogger(Schema.class);

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

        private static final Comparator<ColumnDescriptor> COLUMN_DESCRIPTOR_COMPARATOR = Comparator.comparingInt(ColumnDescriptor::getPosition);

        private TCLIService.Iface client;

        private TOperationHandle operationHandle;

        private List<ColumnDescriptor> columnDescriptors;

        private SchemaBuilder() {
        }

        public SchemaBuilder client(TCLIService.Iface client) {
            this.client = client;
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

            Map<String, ColumnDescriptor> mapping = null;

            if (columnDescriptors != null) {

                mapping = new HashMap<>(columnDescriptors.size());

                for (ColumnDescriptor descriptor : columnDescriptors) {
                    mapping.put(descriptor.getName(), descriptor);
                }
            } else {

                TTableSchema tableSchema = getTableSchema(client, operationHandle);

                List<TColumnDesc> columns = tableSchema.getColumns();

                mapping = new HashMap<>(columns.size());

                for (TColumnDesc columnDesc : columns) {
                    ColumnDescriptor descriptor = ColumnDescriptor.builder().thriftColumn(columnDesc).build();
                    mapping.put(descriptor.getName(), descriptor);
                }

            }

            List<ColumnDescriptor> columns = new ArrayList<>(mapping.values());

            columns.sort(COLUMN_DESCRIPTOR_COMPARATOR);

            return new Schema(mapping, columns, columns.size());
        }


        private static TTableSchema getTableSchema(TCLIService.Iface client, TOperationHandle operationHandle) {
            TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(operationHandle);

            TGetResultSetMetadataResp metadataResp;

            try {
                metadataResp = client.GetResultSetMetadata(metadataReq);
            } catch (TException e) {
                throw new HiveException(e);
            }

            ThriftUtils.checkStatus(metadataResp.getStatus());

            return metadataResp.getSchema();
        }

    }
}
