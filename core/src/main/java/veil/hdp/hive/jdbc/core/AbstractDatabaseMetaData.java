package veil.hdp.hive.jdbc.core;

import veil.hdp.hive.jdbc.HiveDriver;

import java.sql.*;

abstract class AbstractDatabaseMetaData implements DatabaseMetaData {
    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getURL() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getUserName() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getDriverName() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getDriverVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxConnections() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxStatements() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
