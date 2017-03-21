package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.*;

import static org.apache.hive.service.cli.thrift.TCLIService.Client;
import static org.apache.hive.service.cli.thrift.TStatusCode.SUCCESS_STATUS;
import static org.apache.hive.service.cli.thrift.TStatusCode.SUCCESS_WITH_INFO_STATUS;
import static org.slf4j.LoggerFactory.getLogger;

public class HiveServiceUtils {

    private static final Logger log = getLogger(HiveServiceUtils.class);

    private static void checkStatus(TStatus status) throws SQLException {

        if (status.getStatusCode() == SUCCESS_STATUS || status.getStatusCode() == SUCCESS_WITH_INFO_STATUS) {
            return;
        }

        throw new HiveSQLException(status);
    }

    public static TRowSet fetchResults(Client client, TOperationHandle operationHandle, TFetchOrientation orientation, int fetchSize) throws TException, SQLException {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, orientation, fetchSize);
        fetchReq.setFetchType((short) 0);
        TFetchResultsResp fetchResults = client.FetchResults(fetchReq);

        checkStatus(fetchResults.getStatus());

        if (log.isDebugEnabled()) {
            log.debug(fetchResults.toString());
        }

        return fetchResults.getResults();
    }

    public static List<String> fetchLogs(Client client, TOperationHandle operationHandle, TProtocolVersion protocolVersion) {

        List<String> logs = new ArrayList<>();

        TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(operationHandle, TFetchOrientation.FETCH_FIRST, Integer.MAX_VALUE);
        tFetchResultsReq.setFetchType((short) 1);

        try {
            TFetchResultsResp fetchResults = client.FetchResults(tFetchResultsReq);

            checkStatus(fetchResults.getStatus());


            if (log.isDebugEnabled()) {
                log.debug(fetchResults.toString());
            }

            RowSet rowSet = RowSetFactory.create(fetchResults.getResults(), protocolVersion);

            for (Object[] row : rowSet) {
                logs.add(String.valueOf(row[0]));
            }

        } catch (SQLException | TException e) {
            log.error("error fetching logs: {}", e.getMessage(), e);
        }


        return logs;
    }

    public static void closeOperation(Client client, TOperationHandle operationHandle) {
        TCloseOperationReq closeRequest = new TCloseOperationReq(operationHandle);

        try {
            TCloseOperationResp resp = client.CloseOperation(closeRequest);

            checkStatus(resp.getStatus());

            if (log.isDebugEnabled()) {
                log.debug(closeRequest.toString());
            }

        } catch (SQLException | TException e) {
            log.warn(e.getMessage(), e);
        }
    }

    public static void cancelOperation(Client client, TOperationHandle operationHandle) {
        TCancelOperationReq cancelRequest = new TCancelOperationReq(operationHandle);

        try {
            TCancelOperationResp resp = client.CancelOperation(cancelRequest);

            checkStatus(resp.getStatus());

            if (log.isDebugEnabled()) {
                log.debug(cancelRequest.toString());
            }

        } catch (SQLException | TException e) {
            log.warn(e.getMessage(), e);
        }
    }

    public static void closeSession(Client client, TSessionHandle sessionHandle) {
        TCloseSessionReq closeRequest = new TCloseSessionReq(sessionHandle);

        try {
            TCloseSessionResp resp = client.CloseSession(closeRequest);

            checkStatus(resp.getStatus());

            if (log.isDebugEnabled()) {
                log.debug(closeRequest.toString());
            }

        } catch (SQLException | TException e) {
            log.warn(e.getMessage(), e);
        }

    }

    public static TOperationHandle executeSql(Client client, TSessionHandle sessionHandle, long queryTimeout, String sql) throws TException, SQLException {
        TExecuteStatementReq executeStatementReq = new TExecuteStatementReq(sessionHandle, sql);
        executeStatementReq.setRunAsync(true);
        executeStatementReq.setQueryTimeout(queryTimeout);
        //allows per statement configuration of session handle
        //executeStatementReq.setConfOverlay(null);

        TExecuteStatementResp executeStatementResp = client.ExecuteStatement(executeStatementReq);

        checkStatus(executeStatementResp.getStatus());

        if (log.isDebugEnabled()) {
            log.debug(executeStatementResp.toString());
        }

        return executeStatementResp.getOperationHandle();


    }

    public static void waitForStatementToComplete(Client client, TOperationHandle statementHandle) throws TException, SQLException {
        boolean isComplete = false;

        while (!isComplete) {

            TGetOperationStatusReq statusReq = new TGetOperationStatusReq(statementHandle);
            TGetOperationStatusResp statusResp = client.GetOperationStatus(statusReq);

            checkStatus(statusResp.getStatus());

            if (statusResp.isSetOperationState()) {

                switch (statusResp.getOperationState()) {
                    case CLOSED_STATE:
                    case FINISHED_STATE:
                        isComplete = true;
                        break;
                    case CANCELED_STATE:
                        throw new SQLException("Query was cancelled", "01000");
                    case TIMEDOUT_STATE:
                        throw new SQLTimeoutException("Query timed out");
                    case ERROR_STATE:
                        throw new SQLException(statusResp.getErrorMessage(), statusResp.getSqlState(), statusResp.getErrorCode());
                    case UKNOWN_STATE:
                        throw new SQLException("Unknown query", "HY000");
                    case INITIALIZED_STATE:
                    case PENDING_STATE:
                    case RUNNING_STATE:
                        break;
                }
            }

        }
    }


    public static TOpenSessionResp openSession(Properties properties, Client client) throws TException, SQLException {
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        String username = HiveDriverProperty.USER.get(properties);

        if (username != null) {
            openSessionReq.setUsername(username);
            openSessionReq.setPassword(HiveDriverProperty.PASSWORD.get(properties));
        }

        // set properties for session
        Map<String, String> configuration = buildSessionConfig(properties);

        if (log.isDebugEnabled()) {
            log.debug("configuration for session provided to thrift {}", configuration);
        }

        openSessionReq.setConfiguration(configuration);

        if (log.isDebugEnabled()) {
            log.debug(openSessionReq.toString());
        }

        TOpenSessionResp resp = client.OpenSession(openSessionReq);

        checkStatus(resp.getStatus());

        return resp;

    }


    private static Map<String, String> buildSessionConfig(Properties properties) {
        Map<String, String> openSessionConfig = new HashMap<>();

        for (String property : properties.stringPropertyNames()) {
            // no longer going to use HiveConf.ConfVars to validate properties.  it requires too many dependencies.  let server side deal with this.
            if (property.startsWith("hive.")) {
                openSessionConfig.put("set:hiveconf:" + property, properties.getProperty(property));
            }
        }

        openSessionConfig.put("use:database", HiveDriverProperty.DATABASE_NAME.get(properties));

        return openSessionConfig;
    }

    public static TTableSchema getResultSetSchema(Client client, TOperationHandle operationHandle) throws TException, SQLException {

        TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(operationHandle);
        TGetResultSetMetadataResp metadataResp = client.GetResultSetMetadata(metadataReq);

        checkStatus(metadataResp.getStatus());

        if (log.isDebugEnabled()) {
            log.debug(metadataResp.toString());
        }

        return metadataResp.getSchema();
    }

    @Deprecated
    public static void printInfo(Client client, TSessionHandle sessionHandle) throws TException, SQLException {

        for (TGetInfoType tGetInfoType : TGetInfoType.values()) {

            TGetInfoResp serverInfo = getServerInfo(client, sessionHandle, tGetInfoType);

            String value = serverInfo.getInfoValue().getStringValue();

            log.debug("Key {} Value {}", tGetInfoType.toString(), value);
        }

    }

    // todo: this freaks out on the backend if type is not correct and results in failing subsequent calls.  should avoid this until fixed.
    // see: org.apache.hive.service.cli.session.HiveSessionImpl; don't know why it doesn't support more types
    @Deprecated
    public static TGetInfoResp getServerInfo(Client client, TSessionHandle sessionHandle, TGetInfoType type) throws TException, SQLException {
        TGetInfoReq req = new TGetInfoReq(sessionHandle, type);
        TGetInfoResp resp = client.GetInfo(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    public static HiveResultSet getCatalogs(HiveConnection connection) throws SQLException {

        HiveResultSet resultSet = null;

        try {
            TGetCatalogsResp response = getCatalogsResponse(connection.getClient(), connection.getSessionHandle());
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            log.error(e.getMessage(), e);
        }


        return resultSet;
    }

    public static HiveResultSet getSchemas(HiveConnection connection, String catalog, String schemaPattern) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetSchemasResp response = getDatabaseSchemaResponse(connection.getClient(), connection.getSessionHandle(), catalog, schemaPattern);
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    public static HiveResultSet getTypeInfo(HiveConnection connection) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetTypeInfoResp response = getTypeInfoResponse(connection.getClient(), connection.getSessionHandle());
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    public static HiveResultSet getTableTypes(HiveConnection connection) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetTableTypesResp response = getTableTypesResponse(connection.getClient(), connection.getSessionHandle());
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    public static HiveResultSet getTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String types[]) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetTablesResp response = getTablesResponse(connection.getClient(), connection.getSessionHandle(), catalog, schemaPattern, tableNamePattern, types);
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    public static HiveResultSet getColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetColumnsResp response = getColumnsResponse(connection.getClient(), connection.getSessionHandle(), catalog, schemaPattern, tableNamePattern, columnNamePattern);
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    public static HiveResultSet getFunctions(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern) throws SQLException {

        HiveResultSet resultSet;

        try {
            TGetFunctionsResp response = getFunctionsResponse(connection.getClient(), connection.getSessionHandle(), catalog, schemaPattern, functionNamePattern);
            resultSet = buildResultSet(connection, response.getOperationHandle());
        } catch (TException e) {
            throw new SQLException(e);
        }


        return resultSet;
    }

    private static HiveResultSet buildResultSet(HiveConnection connection, TOperationHandle operationHandle) throws SQLException {

        try {
            TableSchema tableSchema = new TableSchema(getResultSetSchema(connection.getClient(), operationHandle));

            log.debug(tableSchema.toString());

            return new HiveResultSet(connection, new HiveStatement(connection), operationHandle, tableSchema);
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    private static TGetCatalogsResp getCatalogsResponse(Client client, TSessionHandle sessionHandle) throws TException, SQLException {
        TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle);
        TGetCatalogsResp resp = client.GetCatalogs(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    private static TGetColumnsResp getColumnsResponse(Client client, TSessionHandle sessionHandle, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws TException, SQLException {
        TGetColumnsReq req = new TGetColumnsReq(sessionHandle);
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);
        req.setColumnName(columnNamePattern == null ? "%" : columnNamePattern);

        TGetColumnsResp resp = client.GetColumns(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    private static TGetFunctionsResp getFunctionsResponse(Client client, TSessionHandle sessionHandle, String catalog, String schemaPattern, String functionNamePattern) throws TException, SQLException {
        TGetFunctionsReq req = new TGetFunctionsReq();
        req.setSessionHandle(sessionHandle);
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setFunctionName(functionNamePattern == null ? "%" : functionNamePattern);

        TGetFunctionsResp resp = client.GetFunctions(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }


    private static TGetTablesResp getTablesResponse(Client client, TSessionHandle sessionHandle, String catalog, String schemaPattern, String tableNamePattern, String types[]) throws TException, SQLException {
        TGetTablesReq req = new TGetTablesReq(sessionHandle);

        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);

        if (types != null) {
            req.setTableTypes(Arrays.asList(types));
        }

        TGetTablesResp resp = client.GetTables(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }


    private static TGetTypeInfoResp getTypeInfoResponse(Client client, TSessionHandle sessionHandle) throws TException, SQLException {
        TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle);
        TGetTypeInfoResp resp = client.GetTypeInfo(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    private static TGetTableTypesResp getTableTypesResponse(Client client, TSessionHandle sessionHandle) throws TException, SQLException {
        TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle);
        TGetTableTypesResp resp = client.GetTableTypes(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    private static TGetSchemasResp getDatabaseSchemaResponse(Client client, TSessionHandle sessionHandle, String catalog, String schemaPattern) throws TException, SQLException {
        TGetSchemasReq req = new TGetSchemasReq(sessionHandle);
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);

        TGetSchemasResp resp = client.GetSchemas(req);

        if (log.isDebugEnabled()) {
            log.debug(resp.toString());
        }

        checkStatus(resp.getStatus());


        return resp;
    }

    public static ResultSet getPrimaryKeys(HiveConnection connection, String catalog, String schema, String table) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.PRIMARY_KEYS));
    }

    public static ResultSet getProcedureColumns(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.PROCEDURE_COLUMNS));
    }

    public static ResultSet getProcedures(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.PROCEDURES));
    }

    public static ResultSet getColumnPrivileges(HiveConnection connection, String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.COLUMN_PRIVILEGES));
    }

    public static ResultSet getTablePrivileges(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.TABLE_PRIVILEGES));
    }

    public static ResultSet getBestRowIdentifier(HiveConnection connection, String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.BEST_ROW_IDENTIFIER));
    }

    public static ResultSet getVersionColumns(HiveConnection connection, String catalog, String schema, String table) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.VERSION_COLUMNS));
    }

    public static ResultSet getImportedKeys(HiveConnection connection, String catalog, String schema, String table) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.IMPORTED_KEYS));
    }

    public static ResultSet getExportedKeys(HiveConnection connection, String catalog, String schema, String table) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.EXPORTED_KEYS));
    }

    public static ResultSet getCrossReference(HiveConnection connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.CROSS_REFERENCE));
    }

    public static ResultSet getIndexInfo(HiveConnection connection, String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.INDEX_INFO));
    }

    public static ResultSet getUDTs(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.UDT));
    }

    public static ResultSet getSuperTypes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.SUPER_TYPES));
    }

    public static ResultSet getSuperTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.SUPER_TABLES));
    }

    public static ResultSet getAttributes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.ATTRIBUTES));
    }

    public static ResultSet getClientInfoProperties(HiveConnection connection) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.CLIENT_INFO_PROPERTIES));
    }

    public static ResultSet getFunctionColumns(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.FUNCTION_COLUMNS));
    }

    public static ResultSet getPseudoColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.PSEUDO_COLUMNS));
    }

    public static ResultSet getGeneratedKeys(HiveConnection connection) throws SQLException {
        return new HiveResultSet(connection, new HiveStatement(connection), null, new TableSchema(ColumnDescriptors.PSEUDO_COLUMNS));
    }
}
