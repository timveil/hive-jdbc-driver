package veil.hdp.hive.jdbc.metadata;

import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TColumnDesc;

public class ColumnDescriptor {

    private final String name;
    private final String comment;
    private final Type type;
    private final int position;

    public ColumnDescriptor(TColumnDesc tColumnDesc) {

        name = tColumnDesc.getColumnName();
        comment = tColumnDesc.getComment();
        type = new TypeDescriptor(tColumnDesc.getTypeDesc()).getType();
        position = tColumnDesc.getPosition();
    }

    public String getName() {
        return name;
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
}
