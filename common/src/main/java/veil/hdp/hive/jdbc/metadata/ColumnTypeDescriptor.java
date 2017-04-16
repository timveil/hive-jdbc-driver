package veil.hdp.hive.jdbc.metadata;


import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TTypeDesc;
import org.apache.hive.service.cli.thrift.TTypeEntry;

import java.util.List;

public class ColumnTypeDescriptor {

    private final HiveType hiveType;

    public ColumnTypeDescriptor(HiveType hiveType) {
        this.hiveType = hiveType;
    }

    public ColumnTypeDescriptor(TTypeDesc typeDesc) {
        List<TTypeEntry> typeEntries = typeDesc.getTypes();
        TPrimitiveTypeEntry primitiveTypeEntry = typeEntries.get(0).getPrimitiveEntry();

        this.hiveType = HiveType.valueOf(primitiveTypeEntry.getType());
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
}
