package veil.hdp.hive.jdbc.metadata;


import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TTypeDesc;
import org.apache.hive.service.cli.thrift.TTypeEntry;

import java.util.List;

public class ColumnTypeDescriptor {

    private final HiveType hiveType;

    private ColumnTypeDescriptor(HiveType hiveType) {
        this.hiveType = hiveType;
    }

    public HiveType getHiveType() {
        return hiveType;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("hiveType", hiveType)
                .toString();
    }

    public static class Builder {

        private TTypeDesc typeDesc;
        private HiveType hiveType;

        public ColumnTypeDescriptor.Builder thriftType(TTypeDesc typeDesc) {
            this.typeDesc = typeDesc;
            return this;
        }

        public ColumnTypeDescriptor.Builder hiveType(HiveType hiveType) {
            this.hiveType = hiveType;
            return this;
        }

        public ColumnTypeDescriptor build() {

            if (hiveType != null) {
                return new ColumnTypeDescriptor(hiveType);
            } else {
                List<TTypeEntry> typeEntries = typeDesc.getTypes();
                TPrimitiveTypeEntry primitiveTypeEntry = typeEntries.get(0).getPrimitiveEntry();

                return new ColumnTypeDescriptor(HiveType.valueOf(primitiveTypeEntry.getType()));
            }

        }
    }
}
