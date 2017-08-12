package veil.hdp.hive.jdbc.metadata;


import org.apache.commons.lang3.builder.Builder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import veil.hdp.hive.jdbc.bindings.TColumnDesc;

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

    public String getComment() {
        return comment;
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
        return new ToStringBuilder(this)
                .append("name", name)
                .append("label", label)
                .append("tableName", tableName)
                .append("comment", comment)
                .append("columnTypeDescriptor", columnTypeDescriptor)
                .append("position", position)
                .toString();
    }

    public static class ColumnDescriptorBuilder implements Builder<ColumnDescriptor> {

        private TColumnDesc thriftColumnDescriptor;
        private ColumnTypeDescriptor columnTypeDescriptor;
        private String name;
        private int position;



        private ColumnDescriptorBuilder() {
        }

        public ColumnDescriptorBuilder thriftColumn(TColumnDesc columnDesc) {
            this.thriftColumnDescriptor = columnDesc;
            return this;
        }

        public ColumnDescriptorBuilder typeDescriptor(ColumnTypeDescriptor typeDescriptor) {
            this.columnTypeDescriptor = typeDescriptor;
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

            if (thriftColumnDescriptor != null) {
                String rawName = thriftColumnDescriptor.getColumnName();

                String columnName;
                String tableName = null;

                // when select statement uses AS keyword, HS2/thrift simply returns this as the columnName without anyway to determine table.  this is really a bug on the HS2/Thrift side
                // when AS is not used, the columnName returned from HS2/Thrift is a `.` separated string with table-name.column-name syntax.

                if (rawName.contains(".")) {
                    columnName = rawName.substring(rawName.lastIndexOf('.') + 1);
                    tableName = rawName.substring(0, rawName.lastIndexOf('.'));
                } else{
                    columnName = rawName;
                }

                return new ColumnDescriptor(columnName,
                        null,
                        tableName,
                        thriftColumnDescriptor.getComment(),
                        ColumnTypeDescriptor.builder().thriftType(thriftColumnDescriptor.getTypeDesc()).build(),
                        thriftColumnDescriptor.getPosition());
            } else {
                return new ColumnDescriptor(name, null, null, null, columnTypeDescriptor, position);
            }
        }

    }
}
