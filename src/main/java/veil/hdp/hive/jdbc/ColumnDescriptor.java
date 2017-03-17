package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TColumnDesc;

public class ColumnDescriptor {

    private final String name;
    private final String normalizedName;
    private final String comment;
    private final Type type;
    private final int position;

    public ColumnDescriptor(TColumnDesc tColumnDesc) {
        name = tColumnDesc.getColumnName();
        normalizedName = normalizeName(tColumnDesc.getColumnName());
        comment = tColumnDesc.getComment();
        type = new TypeDescriptor(tColumnDesc.getTypeDesc()).getType();
        position = tColumnDesc.getPosition();
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

    public Type getType() {
        return type;
    }

    public int getPosition() {
        return position;
    }

    private String normalizeName(String name) {

        if (name.contains(".")) {
            name = name.substring(name.lastIndexOf(".") + 1);
        }

        return name.toLowerCase();
    }
}
