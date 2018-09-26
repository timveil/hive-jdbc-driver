/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveResultSetMetaData extends AbstractResultSetMetaData {

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
        return schema.getColumn(column).getLabel();
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

    @Override
    public String getTableName(int column) throws SQLException {
        return schema.getColumn(column).getTableName();
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

            int columnCount = schema.getColumnCount();

            return new HiveResultSetMetaData(schema, columnCount);
        }
    }


}
