package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TColumnDesc;

public class Column {

    private final String name;
    private final String normalizedName;
    private final String comment;
    private final ColumnType columnType;

    // should be 1 based to match ResultSet
    private final int position;


    public Column(String name, String comment, ColumnType columnType, int position) {
        this.name = name;
        this.normalizedName = normalizeName(name);
        this.comment = comment;
        this.columnType = columnType;
        this.position = position;
    }

    public Column(TColumnDesc columnDesc) {
        name = columnDesc.getColumnName();
        normalizedName = normalizeName(columnDesc.getColumnName());
        comment = columnDesc.getComment();
        columnType = new ColumnType(columnDesc.getTypeDesc());
        position = columnDesc.getPosition();
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

    public ColumnType getColumnType() {
        return columnType;
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
