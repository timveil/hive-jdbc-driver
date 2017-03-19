package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumnDesc;

public class ColumnDescriptor {

    private final String name;
    private final String normalizedName;
    private final String comment;
    private final TypeDescriptor typeDescriptor;

    // should be 1 based to match ResultSet
    private final int position;


    public ColumnDescriptor(String name, String comment, TypeDescriptor typeDescriptor, int position) {
        this.name = name;
        this.normalizedName = normalizeName(name);
        this.comment = comment;
        this.typeDescriptor = typeDescriptor;
        this.position = position;
    }

    public ColumnDescriptor(TColumnDesc tColumnDesc) {
        name = tColumnDesc.getColumnName();
        normalizedName = normalizeName(tColumnDesc.getColumnName());
        comment = tColumnDesc.getComment();
        typeDescriptor = new TypeDescriptor(tColumnDesc.getTypeDesc());
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

    public TypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
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
