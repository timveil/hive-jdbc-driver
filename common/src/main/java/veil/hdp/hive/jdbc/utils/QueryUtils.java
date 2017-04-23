package veil.hdp.hive.jdbc.utils;

import org.slf4j.Logger;
import veil.hdp.hive.jdbc.HiveConnection;
import veil.hdp.hive.jdbc.HiveEmptyResultSet;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.slf4j.LoggerFactory.getLogger;

public class QueryUtils {

    private static final Logger log = getLogger(QueryUtils.class);


    public static ResultSet getCatalogs(HiveConnection connection) {
        return ThriftUtils.getCatalogsOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getSchemas(HiveConnection connection, String catalog, String schemaPattern) {
        return ThriftUtils.getDatabaseSchemaOperation(connection.getThriftSession(), catalog, schemaPattern).getResultSet();
    }

    public static ResultSet getTypeInfo(HiveConnection connection) {
        return ThriftUtils.getTypeInfoOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getTableTypes(HiveConnection connection) {
        return ThriftUtils.getTableTypesOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String types[]) {
        return ThriftUtils.getTablesOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, types).getResultSet();
    }

    public static ResultSet getColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return ThriftUtils.getColumnsOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, columnNamePattern).getResultSet();
    }

    public static ResultSet getFunctions(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern) {
        return ThriftUtils.getFunctionsOperation(connection.getThriftSession(), catalog, schemaPattern, functionNamePattern).getResultSet();
    }


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
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PSEUDO_COLUMNS).build()).build();
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


    public static boolean isValid(HiveConnection connection, int timeout) {

        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeout);
            try (ResultSet ignored = statement.executeQuery("SELECT current_database()")) {
                return true;
            }
        } catch (SQLException e) {
            return false;
        }
    }

    public static void setDatabaseSchema(HiveConnection connection, String schema) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("USE " + schema);
        }
    }




}
