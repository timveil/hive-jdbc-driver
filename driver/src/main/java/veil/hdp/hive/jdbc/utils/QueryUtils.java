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

package veil.hdp.hive.jdbc.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.*;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.ThriftOperation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public final class QueryUtils {

    private static final Logger log = LogManager.getLogger(QueryUtils.class);

    private QueryUtils() {
    }

    public static ResultSet getCatalogs(HiveConnection connection) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation catalogsOperation = ThriftUtils.getCatalogsOperation(connection.getThriftSession());

        return HiveResultSet.builder().thriftOperation(catalogsOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getSchemas(HiveConnection connection, String catalog, String schemaPattern) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation databaseSchemaOperation = ThriftUtils.getDatabaseSchemaOperation(connection.getThriftSession(), catalog, schemaPattern);

        return HiveResultSet.builder().thriftOperation(databaseSchemaOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getTypeInfo(HiveConnection connection) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation typeInfoOperation = ThriftUtils.getTypeInfoOperation(connection.getThriftSession());

        return HiveResultSet.builder().thriftOperation(typeInfoOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getTableTypes(HiveConnection connection) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation tableTypesOperation = ThriftUtils.getTableTypesOperation(connection.getThriftSession());

        return HiveResultSet.builder().thriftOperation(tableTypesOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String types[]) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation tablesOperation = ThriftUtils.getTablesOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, types);

        return HiveResultSet.builder().thriftOperation(tablesOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation columnsOperation = ThriftUtils.getColumnsOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, columnNamePattern);

        return HiveResultSet.builder().thriftOperation(columnsOperation).fetchSize(fetchSize).build();
    }

    public static ResultSet getFunctions(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern) {
        int fetchSize = HiveDriverProperty.FETCH_SIZE.getInt(connection.getThriftSession().getProperties());

        ThriftOperation functionsOperation = ThriftUtils.getFunctionsOperation(connection.getThriftSession(), catalog, schemaPattern, functionNamePattern);

        return HiveResultSet.builder().thriftOperation(functionsOperation).fetchSize(fetchSize).build();
    }

    //TODO - NOW AVAILABLE
    public static ResultSet getPrimaryKeys(HiveConnection connection, String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PRIMARY_KEYS).build()).build();
    }

    public static ResultSet getProcedureColumns(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PROCEDURE_COLUMNS).build()).build();
    }

    public static ResultSet getProcedures(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PROCEDURES).build()).build();
    }

    public static ResultSet getColumnPrivileges(HiveConnection connection, String catalog, String schema, String table, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.COLUMN_PRIVILEGES).build()).build();
    }

    public static ResultSet getTablePrivileges(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.TABLE_PRIVILEGES).build()).build();
    }

    public static ResultSet getBestRowIdentifier(HiveConnection connection, String catalog, String schema, String table, int scope, boolean nullable) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.BEST_ROW_IDENTIFIER).build()).build();
    }

    public static ResultSet getVersionColumns(HiveConnection connection, String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.VERSION_COLUMNS).build()).build();
    }

    public static ResultSet getImportedKeys(HiveConnection connection, String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.IMPORTED_KEYS).build()).build();
    }

    public static ResultSet getExportedKeys(HiveConnection connection, String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.EXPORTED_KEYS).build()).build();
    }

    //TODO - NOW AVAILABLE
    public static ResultSet getCrossReference(HiveConnection connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.CROSS_REFERENCE).build()).build();
    }

    public static ResultSet getIndexInfo(HiveConnection connection, String catalog, String schema, String table, boolean unique, boolean approximate) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.INDEX_INFO).build()).build();
    }

    public static ResultSet getUDTs(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.UDT).build()).build();
    }

    public static ResultSet getSuperTypes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.SUPER_TYPES).build()).build();
    }

    public static ResultSet getSuperTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.SUPER_TABLES).build()).build();
    }

    public static ResultSet getAttributes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.ATTRIBUTES).build()).build();
    }

    public static ResultSet getClientInfoProperties(HiveConnection connection) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.CLIENT_INFO_PROPERTIES).build()).build();
    }

    public static ResultSet getFunctionColumns(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.FUNCTION_COLUMNS).build()).build();
    }

    public static ResultSet getPseudoColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PSEUDO_COLUMNS).build()).build();
    }

    public static ResultSet getGeneratedKeys(HiveConnection connection) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.GENERATED_KEYS).build()).build();
    }

    public static String getDatabaseSchema(HiveConnection connection) throws SQLException {

        String schema = null;

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT current_database()")) {
            if (resultSet.next()) {
                schema = resultSet.getString(1);
            }
        }

        return schema;
    }

    public static void setDatabaseSchema(HiveConnection connection, String schema) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("USE " + schema);
        }
    }


}
