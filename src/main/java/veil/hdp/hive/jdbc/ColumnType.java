package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TTypeDesc;
import org.apache.hive.service.cli.thrift.TTypeEntry;

import java.util.List;

public class ColumnType {

    private final Type hiveType;

    public ColumnType(Type hiveType) {
        this.hiveType = hiveType;
    }

    public ColumnType(TTypeDesc typeDesc) {
        List<TTypeEntry> typeEntries = typeDesc.getTypes();
        TPrimitiveTypeEntry primitiveTypeEntry = typeEntries.get(0).getPrimitiveEntry();

        this.hiveType = Type.getType(primitiveTypeEntry.getType());
    }

    public Type getHiveType() {
        return hiveType;
    }

    @Override
    public String toString() {
        return "ColumnType{" +
                "hiveType=" + hiveType +
                '}';
    }
}
