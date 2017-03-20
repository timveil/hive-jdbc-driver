package veil.hdp.hive.jdbc;

import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveDatabaseMetaData extends AbstractDatabaseMetaData {

    private static final Logger log = LoggerFactory.getLogger(HiveDatabaseMetaData.class);

    private final HiveConnection connection;

    HiveDatabaseMetaData(HiveConnection connection) {
        this.connection = connection;
    }


    @Override
    public String getDriverName() throws SQLException {
        return HiveDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return new HiveDriver().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return new HiveDriver().getMinorVersion();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Apache Hive";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return HiveVersionInfo.getVersion();
    }

    // todo: split version
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    // todo: split version
    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    // todo: i don't think this is right, need better way to determine.
    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return HiveServiceUtils.getCatalogs(connection);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return HiveServiceUtils.getSchemas(connection, catalog, schemaPattern);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return HiveServiceUtils.getTypeInfo(connection);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return HiveServiceUtils.getTableTypes(connection);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return HiveServiceUtils.getTables(connection, catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return HiveServiceUtils.getColumns(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    // todo: move to constant
    @Override
    public String getCatalogSeparator() throws SQLException {
        return String.valueOf('.');
    }

    // todo: move to constant; need to research why this value
    @Override
    public String getCatalogTerm() throws SQLException {
        return "instance";
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    // todo: move to constant
    @Override
    public String getSearchStringEscape() throws SQLException {
        return String.valueOf('\\');
    }

    @Override
    public String getURL() throws SQLException {
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getProperties().getProperty(HiveDriverStringProperty.USER.getName());
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    // todo: as of hive 1.2.1 there are no primary keys
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return HiveServiceUtils.getPrimaryKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return HiveServiceUtils.getProcedures(connection, catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return HiveServiceUtils.getProcedureColumns(connection, catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return HiveServiceUtils.getColumnPrivileges(connection, catalog, schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return HiveServiceUtils.getTablePrivileges(connection, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return HiveServiceUtils.getBestRowIdentifier(connection, catalog, schema, table, scope, nullable);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return HiveServiceUtils.getVersionColumns(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return HiveServiceUtils.getImportedKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return HiveServiceUtils.getExportedKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return HiveServiceUtils.getCrossReference(connection, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return HiveServiceUtils.getIndexInfo(connection, catalog, schema, table, unique, approximate);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return HiveServiceUtils.getUDTs(connection, catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return HiveServiceUtils.getSuperTypes(connection, catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return HiveServiceUtils.getSuperTables(connection, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return HiveServiceUtils.getAttributes(connection, catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return HiveServiceUtils.getClientInfoProperties(connection);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return HiveServiceUtils.getFunctions(connection, catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return HiveServiceUtils.getFunctionColumns(connection, catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return HiveServiceUtils.getPseudoColumns(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }
}
