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


import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.TColumnDesc;
import veil.hdp.hive.jdbc.thrift.ThriftSession;

public class ColumnDescriptor {

    private final String name;
    private final String label;
    private final String tableName;
    private final String comment;
    private final ColumnTypeDescriptor columnTypeDescriptor;

    // should be 1 based to match ResultSet
    private final int position;

    private ColumnDescriptor(String name, String label, String tableName, String comment, ColumnTypeDescriptor columnTypeDescriptor, int position) {
        this.name = name;
        this.label = label;
        this.tableName = tableName;
        this.comment = comment;
        this.columnTypeDescriptor = columnTypeDescriptor;
        this.position = position;
    }


    public static ColumnDescriptorBuilder builder() {
        return new ColumnDescriptorBuilder();
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getTableName() {
        return tableName;
    }

    public ColumnTypeDescriptor getColumnType() {
        return columnTypeDescriptor;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "ColumnDescriptor{" +
                "name='" + name + '\'' +
                ", label='" + label + '\'' +
                ", tableName='" + tableName + '\'' +
                ", comment='" + comment + '\'' +
                ", columnTypeDescriptor=" + columnTypeDescriptor +
                ", position=" + position +
                '}';
    }

    public static class ColumnDescriptorBuilder implements Builder<ColumnDescriptor> {

        private static final String DOT = ".";

        private TColumnDesc tColumnDesc;
        private ColumnTypeDescriptor columnTypeDescriptor;
        private String name;
        private int position;
        private ThriftSession session;


        private ColumnDescriptorBuilder() {
        }

        ColumnDescriptorBuilder thriftColumn(TColumnDesc columnDesc) {
            this.tColumnDesc = columnDesc;
            return this;
        }

        public ColumnDescriptorBuilder typeDescriptor(ColumnTypeDescriptor typeDescriptor) {
            this.columnTypeDescriptor = typeDescriptor;
            return this;
        }

        public ColumnDescriptorBuilder session(ThriftSession session) {
            this.session = session;
            return this;
        }

        public ColumnDescriptorBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ColumnDescriptorBuilder position(int position) {
            this.position = position;
            return this;
        }

        public ColumnDescriptor build() {

            if (tColumnDesc != null && session != null) {
                String rawName = tColumnDesc.getColumnName();

                String columnName;
                String tableName = null;

                // when select statement uses AS keyword, HS2/thrift simply returns this as the columnName without anyway to determine table.  this is really a bug on the HS2/Thrift side
                // when AS is not used, the columnName returned from HS2/Thrift is a `.` separated string with table-name.column-name syntax.

                if (rawName.contains(DOT)) {
                    int index = rawName.lastIndexOf(DOT);
                    columnName = rawName.substring(index + 1);
                    tableName = rawName.substring(0, index);
                } else {
                    columnName = rawName;
                }

                columnTypeDescriptor = session.getCachedDescriptor(tColumnDesc.getTypeDesc());

                return new ColumnDescriptor(columnName,
                        null,
                        tableName,
                        tColumnDesc.getComment(),
                        columnTypeDescriptor,
                        tColumnDesc.getPosition());
            } else {
                return new ColumnDescriptor(name, null, null, null, columnTypeDescriptor, position);
            }
        }

    }
}
