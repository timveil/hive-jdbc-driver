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

    public ColumnType(TTypeDesc tTypeDesc) {
        List<TTypeEntry> tTypeEntries = tTypeDesc.getTypes();
        TPrimitiveTypeEntry top = tTypeEntries.get(0).getPrimitiveEntry();
        this.hiveType = Type.getType(top.getType());
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
