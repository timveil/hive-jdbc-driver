package veil.hdp.hive.jdbc;

import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.QueryUtils;

import java.sql.*;

public class HiveDatabaseMetaData extends AbstractDatabaseMetaData {

    private static final Logger log = LoggerFactory.getLogger(HiveDatabaseMetaData.class);

    // constructor
    private final HiveConnection connection;

    private HiveDatabaseMetaData(HiveConnection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
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
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
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
        return Boolean.FALSE;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return QueryUtils.getCatalogs(connection);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return QueryUtils.getSchemas(connection, catalog, schemaPattern);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return QueryUtils.getTypeInfo(connection);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return QueryUtils.getTableTypes(connection);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return QueryUtils.getTables(connection, catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return QueryUtils.getColumns(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
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
        return HiveDriverProperty.USER.get(connection.getThriftSession().getProperties());
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return QueryUtils.getPrimaryKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return QueryUtils.getProcedures(connection, catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return QueryUtils.getProcedureColumns(connection, catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return QueryUtils.getColumnPrivileges(connection, catalog, schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return QueryUtils.getTablePrivileges(connection, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return QueryUtils.getBestRowIdentifier(connection, catalog, schema, table, scope, nullable);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return QueryUtils.getVersionColumns(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return QueryUtils.getImportedKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return QueryUtils.getExportedKeys(connection, catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return QueryUtils.getCrossReference(connection, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return QueryUtils.getIndexInfo(connection, catalog, schema, table, unique, approximate);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return QueryUtils.getUDTs(connection, catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return QueryUtils.getSuperTypes(connection, catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return QueryUtils.getSuperTables(connection, catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return QueryUtils.getAttributes(connection, catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return QueryUtils.getClientInfoProperties(connection);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return QueryUtils.getFunctions(connection, catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return QueryUtils.getFunctionColumns(connection, catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return QueryUtils.getPseudoColumns(connection, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return Boolean.TRUE;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "UDF";
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return Boolean.FALSE;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public static class Builder {

        private HiveConnection connection;

        public HiveDatabaseMetaData.Builder connection(HiveConnection connection) {
            this.connection = connection;
            return this;
        }

        public HiveDatabaseMetaData build() {
            return new HiveDatabaseMetaData(connection);
        }
    }

}
