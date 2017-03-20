package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveResultSetMetaData extends AbstractResultSetMetaData {

    private final Logger log = LoggerFactory.getLogger(HiveResultSetMetaData.class);

    // constructor
    private final TableSchema schema;

    private int columnCount;


    HiveResultSetMetaData(TableSchema schema) {
        this.schema = schema;

        if (schema.getColumns() != null) {
            this.columnCount = schema.getColumns().size();
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnCount;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        // todo: this should be a map of hive type to java, not sql type to java
        return SqlTypeMap.toClass(getColumnType(column)).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return schema.getColumn(column).getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return schema.getColumn(column).getNormalizedName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return schema.getColumn(column).getTypeDescriptor().getType().toJavaSQLType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return schema.getColumn(column).getTypeDescriptor().getType().getName();
    }

    // todo: research this more; don't believe has this concept
    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }


    // todo: research this more; don't believe has this concept
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    // todo: research this more; don't believe has this concept
    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    // todo: how does this work with acid
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    // todo: need to research this more
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    // todo
    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    // todo
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    // todo
    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int columnType = getColumnType(column);

        JDBCType jdbcType = JDBCType.valueOf(columnType);

        boolean signed = false;

        switch (jdbcType) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                signed = true;
                break;
        }

        return signed;
    }

    // todo: need to research this more
    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    // todo: need to research this more
    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    // todo: need to research this more
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    // todo: need to research this more
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    // todo: need to research this more
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }


}
