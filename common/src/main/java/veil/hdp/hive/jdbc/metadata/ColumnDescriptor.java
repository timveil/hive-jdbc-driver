package veil.hdp.hive.jdbc.metadata;


import org.apache.commons.lang3.builder.Builder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hive.service.rpc.thrift.TColumnDesc;

public class ColumnDescriptor {

    private final String name;
    private final String normalizedName;
    private final String comment;
    private final ColumnTypeDescriptor columnTypeDescriptor;

    // should be 1 based to match ResultSet
    private final int position;

    private ColumnDescriptor(String name, String normalizedName, String comment, ColumnTypeDescriptor columnTypeDescriptor, int position) {
        this.name = name;
        this.normalizedName = normalizedName;
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

    public String getNormalizedName() {
        return normalizedName;
    }

    public String getComment() {
        return comment;
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
                .append("normalizedName", normalizedName)
                .append("comment", comment)
                .append("columnTypeDescriptor", columnTypeDescriptor)
                .append("position", position)
                .toString();
    }

    public static class ColumnDescriptorBuilder implements Builder<ColumnDescriptor> {

        private TColumnDesc columnDesc;
        private ColumnTypeDescriptor typeDescriptor;
        private String name;
        private int position;



        private ColumnDescriptorBuilder() {
        }

        public ColumnDescriptorBuilder thriftColumn(TColumnDesc columnDesc) {
            this.columnDesc = columnDesc;
            return this;
        }

        public ColumnDescriptorBuilder typeDescriptor(ColumnTypeDescriptor typeDescriptor) {
            this.typeDescriptor = typeDescriptor;
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

            if (columnDesc != null) {
                String name = columnDesc.getColumnName();

                return new ColumnDescriptor(name, normalizeName(name), columnDesc.getComment(), ColumnTypeDescriptor.builder().thriftType(columnDesc.getTypeDesc()).build(), columnDesc.getPosition());
            } else {
                return new ColumnDescriptor(name, normalizeName(name), null, typeDescriptor, position);
            }
        }


        private static String normalizeName(String name) {

            if (name.contains(".")) {
                name = name.substring(name.lastIndexOf('.') + 1);
            }

            return name.toLowerCase();
        }
    }
}
