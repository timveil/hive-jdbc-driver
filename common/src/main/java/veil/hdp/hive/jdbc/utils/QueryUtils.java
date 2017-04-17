package veil.hdp.hive.jdbc.utils;

import com.google.common.collect.AbstractIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import veil.hdp.hive.jdbc.*;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.data.RowBaseSet;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.slf4j.LoggerFactory.getLogger;

public class QueryUtils {

    private static final Logger log = getLogger(QueryUtils.class);

    private static final short FETCH_TYPE_QUERY = 0;
    private static final short FETCH_TYPE_LOG = 1;


    public static Iterable<Row> getResults(ThriftSession session, TOperationHandle handle, int fetchSize, Schema schema) {
        return () -> {

            final Iterator<List<Row>> fetchIterator = fetchIterator(session, handle, fetchSize, schema);

            return new AbstractIterator<Row>() {

                private final AtomicInteger rowCount = new AtomicInteger(0);
                private Iterator<Row> rowSet;

                @Override
                protected Row computeNext() {
                    while (true) {
                        if (rowSet == null) {
                            if (fetchIterator.hasNext()) {
                                rowSet = fetchIterator.next().iterator();
                            } else {
                                return endOfData();
                            }
                        }

                        if (rowSet.hasNext()) {
                            // the page has more results
                            rowCount.incrementAndGet();
                            return rowSet.next();
                        } else if (rowCount.get() < fetchSize) {
                            // the page has no more results and the rowCount is < fetchSize; then i don't need
                            // to go back to the server to know if i'm done.
                            //
                            // for example rowCount = 10; fetchSize = 100; then no need to look for another page
                            //
                            return endOfData();
                        } else {
                            // the page has no more results, but rowCount = fetchSize.  need to check server for more results
                            rowSet = null;
                            rowCount.set(0);

                        }
                    }
                }
            };
        };
    }

    private static AbstractIterator<List<Row>> fetchIterator(ThriftSession session, TOperationHandle handle, int fetchSize, Schema schema) {
        return new AbstractIterator<List<Row>>() {

            @Override
            protected List<Row> computeNext() {

                List<Row> results = null;

                try {
                    results = QueryUtils.fetchResults(session, handle, TFetchOrientation.FETCH_NEXT, fetchSize, schema);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (results != null && !results.isEmpty()) {
                    return results;
                } else {
                    return endOfData();
                }

            }
        };
    }


    private static List<Row> fetchResults(ThriftSession session, TOperationHandle operationHandle, TFetchOrientation orientation, int fetchSize, Schema schema) throws SQLException {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, orientation, fetchSize);
        fetchReq.setFetchType(FETCH_TYPE_QUERY);

        return getRows(session, schema, fetchReq);
    }

    private static List<Row> fetchLogs(ThriftSession session, TOperationHandle operationHandle, Schema schema) throws SQLException {

        TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(operationHandle, TFetchOrientation.FETCH_FIRST, Integer.MAX_VALUE);
        tFetchResultsReq.setFetchType(FETCH_TYPE_LOG);

        return getRows(session, schema, tFetchResultsReq);

    }

    private static List<Row> getRows(ThriftSession session, Schema schema, TFetchResultsReq tFetchResultsReq) throws SQLException {
        TFetchResultsResp fetchResults;

        session.getSessionLock().lock();

        try {
            fetchResults = session.getClient().FetchResults(tFetchResultsReq);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(fetchResults.getStatus());

        return getRows(fetchResults.getResults(), schema);


    }

    public static ThriftOperation executeSql(ThriftSession session, String sql, long queryTimeout, int fetchSize, int maxRows) throws SQLException {
        TExecuteStatementReq executeStatementReq = new TExecuteStatementReq(session.getSessionHandle(), StringUtils.trim(sql));
        executeStatementReq.setRunAsync(true);
        executeStatementReq.setQueryTimeout(queryTimeout);
        //todo: allows per statement configuration of session handle
        //executeStatementReq.setConfOverlay(null);

        TExecuteStatementResp executeStatementResp;

        session.getSessionLock().lock();

        try {
            executeStatementResp = session.getClient().ExecuteStatement(executeStatementReq);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(executeStatementResp.getStatus());

        waitForStatementToComplete(session, executeStatementResp.getOperationHandle());

        return new ThriftOperation.Builder().session(session).handle(executeStatementResp.getOperationHandle()).maxRows(maxRows).fetchSize(fetchSize).build();

    }

    private static void waitForStatementToComplete(ThriftSession session, TOperationHandle handle) throws SQLException {
        boolean isComplete = false;

        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(handle);

        while (!isComplete) {


            TGetOperationStatusResp statusResp;

            session.getSessionLock().lock();

            try {
                statusResp = session.getClient().GetOperationStatus(statusReq);
            } catch (TException e) {
                throw new HiveSQLException(e);
            } finally {
                session.getSessionLock().unlock();
            }

            checkStatus(statusResp.getStatus());

            if (statusResp.isSetOperationState()) {

                switch (statusResp.getOperationState()) {
                    case CLOSED_STATE:
                        throw new HiveSQLException("The thriftOperation was closed by a client");
                    case FINISHED_STATE:
                        isComplete = true;
                        break;
                    case CANCELED_STATE:
                        throw new HiveSQLException("The thriftOperation was canceled by a client");
                    case TIMEDOUT_STATE:
                        throw new SQLTimeoutException("The thriftOperation timed out");
                    case ERROR_STATE:
                        throw new HiveSQLException(statusResp.getErrorMessage(), statusResp.getSqlState(), statusResp.getErrorCode());
                    case UKNOWN_STATE:
                        throw new HiveSQLException("The thriftOperation is in an unrecognized state");
                    case INITIALIZED_STATE:
                    case PENDING_STATE:
                    case RUNNING_STATE:
                        break;
                }
            }


        }
    }

    public static TTableSchema getResultSetSchema(ThriftSession session, TOperationHandle handle) throws SQLException {

        TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(handle);

        TGetResultSetMetadataResp metadataResp;

        session.getSessionLock().lock();

        try {
            metadataResp = session.getClient().GetResultSetMetadata(metadataReq);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(metadataResp.getStatus());

        return metadataResp.getSchema();


    }


    public static ResultSet getCatalogs(HiveConnection connection) throws SQLException {
        return getCatalogsOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getSchemas(HiveConnection connection, String catalog, String schemaPattern) throws SQLException {
        return getDatabaseSchemaOperation(connection.getThriftSession(), catalog, schemaPattern).getResultSet();
    }

    public static ResultSet getTypeInfo(HiveConnection connection) throws SQLException {
        return getTypeInfoOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getTableTypes(HiveConnection connection) throws SQLException {
        return getTableTypesOperation(connection.getThriftSession()).getResultSet();
    }

    public static ResultSet getTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String types[]) throws SQLException {
        return getTablesOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, types).getResultSet();
    }

    public static ResultSet getColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return getColumnsOperation(connection.getThriftSession(), catalog, schemaPattern, tableNamePattern, columnNamePattern).getResultSet();
    }

    public static ResultSet getFunctions(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return getFunctionsOperation(connection.getThriftSession(), catalog, schemaPattern, functionNamePattern).getResultSet();
    }


    private static ThriftOperation getCatalogsOperation(ThriftSession session) throws SQLException {
        TGetCatalogsReq req = new TGetCatalogsReq(session.getSessionHandle());

        TGetCatalogsResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetCatalogs(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();


    }

    private static ThriftOperation getColumnsOperation(ThriftSession session, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        TGetColumnsReq req = new TGetColumnsReq(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);
        req.setColumnName(columnNamePattern == null ? "%" : columnNamePattern);

        TGetColumnsResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetColumns(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();

    }

    private static ThriftOperation getFunctionsOperation(ThriftSession session, String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        TGetFunctionsReq req = new TGetFunctionsReq();
        req.setSessionHandle(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setFunctionName(functionNamePattern == null ? "%" : functionNamePattern);

        TGetFunctionsResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetFunctions(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();


    }


    private static ThriftOperation getTablesOperation(ThriftSession session, String catalog, String schemaPattern, String tableNamePattern, String types[]) throws SQLException {
        TGetTablesReq req = new TGetTablesReq(session.getSessionHandle());

        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);

        if (types != null) {
            req.setTableTypes(Arrays.asList(types));
        }

        TGetTablesResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetTables(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();


    }


    private static ThriftOperation getTypeInfoOperation(ThriftSession session) throws SQLException {
        TGetTypeInfoReq req = new TGetTypeInfoReq(session.getSessionHandle());

        TGetTypeInfoResp resp;

        session.getSessionLock().lock();


        try {
            resp = session.getClient().GetTypeInfo(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();


    }

    private static ThriftOperation getTableTypesOperation(ThriftSession session) throws SQLException {
        TGetTableTypesReq req = new TGetTableTypesReq(session.getSessionHandle());

        TGetTableTypesResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetTableTypes(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();

    }

    private static ThriftOperation getDatabaseSchemaOperation(ThriftSession session, String catalog, String schemaPattern) throws SQLException {
        TGetSchemasReq req = new TGetSchemasReq(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);

        TGetSchemasResp resp;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().GetSchemas(req);
        } catch (TException e) {
            throw new HiveSQLException(e);
        } finally {
            session.getSessionLock().unlock();
        }

        checkStatus(resp.getStatus());

        return new ThriftOperation.Builder().handle(resp.getOperationHandle()).metaData(true).session(session).build();


    }

    public static ResultSet getPrimaryKeys(HiveConnection connection, String catalog, String schema, String table) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.PRIMARY_KEYS).build()).build();
    }

    public static ResultSet getProcedureColumns(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.PROCEDURE_COLUMNS).build()).build();
    }

    public static ResultSet getProcedures(HiveConnection connection, String catalog, String schemaPattern, String procedureNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.PROCEDURES).build()).build();
    }

    public static ResultSet getColumnPrivileges(HiveConnection connection, String catalog, String schema, String table, String columnNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.COLUMN_PRIVILEGES).build()).build();
    }

    public static ResultSet getTablePrivileges(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.TABLE_PRIVILEGES).build()).build();
    }

    public static ResultSet getBestRowIdentifier(HiveConnection connection, String catalog, String schema, String table, int scope, boolean nullable) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.BEST_ROW_IDENTIFIER).build()).build();
    }

    public static ResultSet getVersionColumns(HiveConnection connection, String catalog, String schema, String table) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.VERSION_COLUMNS).build()).build();
    }

    public static ResultSet getImportedKeys(HiveConnection connection, String catalog, String schema, String table) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.IMPORTED_KEYS).build()).build();
    }

    public static ResultSet getExportedKeys(HiveConnection connection, String catalog, String schema, String table) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.EXPORTED_KEYS).build()).build();
    }

    public static ResultSet getCrossReference(HiveConnection connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.CROSS_REFERENCE).build()).build();
    }

    public static ResultSet getIndexInfo(HiveConnection connection, String catalog, String schema, String table, boolean unique, boolean approximate) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.INDEX_INFO).build()).build();
    }

    public static ResultSet getUDTs(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.UDT).build()).build();
    }

    public static ResultSet getSuperTypes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.SUPER_TYPES).build()).build();
    }

    public static ResultSet getSuperTables(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.SUPER_TABLES).build()).build();
    }

    public static ResultSet getAttributes(HiveConnection connection, String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.ATTRIBUTES).build()).build();
    }

    public static ResultSet getClientInfoProperties(HiveConnection connection) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.CLIENT_INFO_PROPERTIES).build()).build();
    }

    public static ResultSet getFunctionColumns(HiveConnection connection, String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.FUNCTION_COLUMNS).build()).build();
    }

    public static ResultSet getPseudoColumns(HiveConnection connection, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.PSEUDO_COLUMNS).build()).build();
    }

    public static ResultSet getGeneratedKeys(HiveConnection connection) {
        return new HiveEmptyResultSet.Builder().schema(new Schema.Builder().descriptors(StaticColumnDescriptors.PSEUDO_COLUMNS).build()).build();
    }

    public static String getSchema(HiveConnection connection) throws SQLException {

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

    public static void setSchema(HiveConnection connection, String schema) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("USE " + schema);
        }
    }

    static void checkStatus(TStatus status) throws SQLException {

        if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
            return;
        }

        throw new HiveSQLException(status);
    }

    public static void closeOperation(ThriftOperation operation) {
        closeOperation(operation.getSession(), operation.getOperationHandle());
    }

    public static void closeOperation(ThriftSession session, TOperationHandle handle) {
        TCloseOperationReq closeRequest = new TCloseOperationReq(handle);

        TCloseOperationResp resp = null;

        session.getSessionLock().lock();

        try {
            resp = session.getClient().CloseOperation(closeRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        } finally {
            session.getSessionLock().unlock();
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (SQLException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }

    }

    public static void cancelOperation(ThriftOperation operation) {
        TCancelOperationReq cancelRequest = new TCancelOperationReq(operation.getOperationHandle());

        TCancelOperationResp resp = null;

        operation.getSession().getSessionLock().lock();

        try {
            resp = operation.getSession().getClient().CancelOperation(cancelRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        } finally {
            operation.getSession().getSessionLock().unlock();
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (SQLException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }
    }

    public static void closeSession(ThriftSession thriftSession) {
        TCloseSessionReq closeRequest = new TCloseSessionReq(thriftSession.getSessionHandle());

        TCloseSessionResp resp = null;

        thriftSession.getSessionLock().lock();

        try {
            resp = thriftSession.getClient().CloseSession(closeRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        } finally {
            thriftSession.getSessionLock().unlock();
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (SQLException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }

    }

    private static List<Row> getRows(TRowSet rowSet, Schema schema) {

        List<Row> rows = null;

        if (rowSet != null && rowSet.isSetColumns()) {

            ColumnBasedSet cbs = new ColumnBasedSet.Builder().rowSet(rowSet).schema(schema).build();

            rows = new RowBaseSet.Builder().columnBaseSet(cbs).build().getRows();

        }

        return rows;

    }


}
