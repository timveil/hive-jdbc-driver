package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TTypeDesc;
import org.apache.hive.service.cli.thrift.TTypeEntry;

import java.util.List;

public class ColumnType {

    private final HiveType hiveType;

    public ColumnType(HiveType hiveType) {
        this.hiveType = hiveType;
    }

    public ColumnType(TTypeDesc typeDesc) {
        List<TTypeEntry> typeEntries = typeDesc.getTypes();
        TPrimitiveTypeEntry primitiveTypeEntry = typeEntries.get(0).getPrimitiveEntry();

        this.hiveType = HiveType.valueOf(primitiveTypeEntry.getType());
    }

    public HiveType getHiveType() {
        return hiveType;
    }

    @Override
    public String toString() {
        return "ColumnType{" +
                "hiveType=" + hiveType +
                '}';
    }
}
