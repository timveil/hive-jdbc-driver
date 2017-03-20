package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TTypeDesc;
import org.apache.hive.service.cli.thrift.TTypeEntry;

import java.util.List;

public class TypeDescriptor {

    private final Type type;

    public TypeDescriptor(Type type) {
        this.type = type;
    }

    public TypeDescriptor(TTypeDesc tTypeDesc) {
        List<TTypeEntry> tTypeEntries = tTypeDesc.getTypes();
        TPrimitiveTypeEntry top = tTypeEntries.get(0).getPrimitiveEntry();
        this.type = Type.getType(top.getType());
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "TypeDescriptor{" +
                "type=" + type +
                '}';
    }
}
