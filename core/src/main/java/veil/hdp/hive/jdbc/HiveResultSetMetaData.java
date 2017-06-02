package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.metadata.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveResultSetMetaData extends AbstractResultSetMetaData {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSetMetaData.class);

    // constructor
    private final Schema schema;
    private final int columnCount;


    private HiveResultSetMetaData(Schema schema, int columnCount) {
        this.schema = schema;
        this.columnCount = columnCount;
    }

    public static HiveResultSetMetaDataBuilder builder() {
        return new HiveResultSetMetaDataBuilder();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnCount;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().getJavaType().getName();
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
        return schema.getColumn(column).getColumnType().getHiveType().getJdbcType().getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().getName();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().isCaseSensitive();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().getPrecision();
    }

    // todo; why do i really care about this?
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().getScale();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().isSearchable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return schema.getColumn(column).getColumnType().getHiveType().isSigned();
    }

    // todo: need to research this more; where would i get this from
    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    // todo: need to research this more; where would i get this from
    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    // todo: need to research this more; where would i get this from
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return Boolean.FALSE;
    }

    public static class HiveResultSetMetaDataBuilder implements Builder<HiveResultSetMetaData> {

        private Schema schema;

        private HiveResultSetMetaDataBuilder() {
        }

        public HiveResultSetMetaDataBuilder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public HiveResultSetMetaData build() {

            int columnCount = 0;

            if (schema.getColumns() != null) {
                columnCount = schema.getColumns().size();
            }

            return new HiveResultSetMetaData(schema, columnCount);
        }
    }


}
