package veil.hdp.hive.jdbc.utils;

import com.google.common.collect.AbstractIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.ClientInvocationHandler;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.bindings.TCLIService.Client;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.data.RowBaseSet;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.thrift.*;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public final class ThriftUtils {

    private static final Logger log = LogManager.getLogger(ThriftUtils.class);

    private static final short FETCH_TYPE_QUERY = 0;
    private static final short FETCH_TYPE_LOG = 1;

    private ThriftUtils() {
    }

    public static void openTransport(TTransport transport, int timeout) {

        try {
            ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "open-thrift-transport-thread"));

            Future<Boolean> future = executor.submit(() -> {
                transport.open();
                return true;
            });

            future.get(timeout, TimeUnit.MILLISECONDS);

        } catch (InterruptedException | ExecutionException e) {
            throw new HiveException(e);
        } catch (TimeoutException e) {
            throw new HiveException("The Thrift Transport did not open prior to Timeout.  If using Kerberos, double check that you have a valid client Principal by running klist.", e);
        }

    }


    public static TCLIService.Iface createClient(ThriftTransport transport) {
        TCLIService.Iface client = new Client(new TBinaryProtocol(transport.getTransport()));

        return (TCLIService.Iface) Proxy.newProxyInstance(ThriftUtils.class.getClassLoader(), new Class[]{TCLIService.Iface.class}, new ClientInvocationHandler(client));
    }


    public static TOpenSessionResp openSession(Properties properties, TCLIService.Iface client, TProtocolVersion protocolVersion) throws InvalidProtocolException {


        TOpenSessionReq openSessionReq = new TOpenSessionReq(protocolVersion);

        // set properties for session
        Map<String, String> configuration = buildSessionConfig(properties);

        openSessionReq.setConfiguration(configuration);

        try {
            TOpenSessionResp resp = client.OpenSession(openSessionReq);

            checkStatus(resp.getStatus());

            return resp;
        } catch (TException e) {
            if (StringUtils.containsIgnoreCase(e.getMessage(), "'client_protocol' is unset")) {
                // this often happens when the protocol of the driver is higher than supported by the server.  in other words, driver is newer than server.  handle this gracefully.
                throw new InvalidProtocolException(e);
            } else {
                throw new HiveThriftException(e);
            }
        }

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

    public static void closeSession(ThriftSession thriftSession) {
        TCloseSessionReq closeRequest = new TCloseSessionReq(thriftSession.getSessionHandle());

        TCloseSessionResp resp = null;

        TCLIService.Iface client = thriftSession.getClient();

        try {
            resp = client.CloseSession(closeRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (HiveThriftException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }

    }

    public static void closeOperation(ThriftOperation operation) {
        closeOperation(operation.getSession(), operation.getOperationHandle());
    }

    public static void closeOperation(ThriftSession session, TOperationHandle handle) {
        TCloseOperationReq closeRequest = new TCloseOperationReq(handle);

        TCloseOperationResp resp = null;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.CloseOperation(closeRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (HiveThriftException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }

    }

    public static void cancelOperation(ThriftOperation operation) {
        TCancelOperationReq cancelRequest = new TCancelOperationReq(operation.getOperationHandle());

        TCancelOperationResp resp = null;

        TCLIService.Iface client = operation.getSession().getClient();

        try {
            resp = client.CancelOperation(cancelRequest);
        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }

        if (resp != null) {
            try {
                checkStatus(resp.getStatus());
            } catch (HiveThriftException e) {
                log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
            }
        }
    }

    static ThriftOperation getCatalogsOperation(ThriftSession session, int fetchSize) {
        TGetCatalogsReq req = new TGetCatalogsReq(session.getSessionHandle());

        TGetCatalogsResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetCatalogs(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();


    }

    static ThriftOperation getColumnsOperation(ThriftSession session, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, int fetchSize) {
        TGetColumnsReq req = new TGetColumnsReq(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);
        req.setColumnName(columnNamePattern == null ? "%" : columnNamePattern);

        TGetColumnsResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetColumns(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();

    }

    static ThriftOperation getFunctionsOperation(ThriftSession session, String catalog, String schemaPattern, String functionNamePattern, int fetchSize) {
        TGetFunctionsReq req = new TGetFunctionsReq();
        req.setSessionHandle(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setFunctionName(functionNamePattern == null ? "%" : functionNamePattern);

        TGetFunctionsResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetFunctions(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();


    }

    static ThriftOperation getTablesOperation(ThriftSession session, String catalog, String schemaPattern, String tableNamePattern, String[] types, int fetchSize) {
        TGetTablesReq req = new TGetTablesReq(session.getSessionHandle());

        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);
        req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);

        if (types != null) {
            req.setTableTypes(Arrays.asList(types));
        }

        TGetTablesResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetTables(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();


    }

    static ThriftOperation getTypeInfoOperation(ThriftSession session, int fetchSize) {
        TGetTypeInfoReq req = new TGetTypeInfoReq(session.getSessionHandle());

        TGetTypeInfoResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetTypeInfo(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();


    }

    public static TGetInfoValue getServerInfo(ThriftSession session, TGetInfoType type) {
        TGetInfoReq req = new TGetInfoReq(session.getSessionHandle(), type);

        TGetInfoResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetInfo(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return resp.getInfoValue();

    }

    static ThriftOperation getTableTypesOperation(ThriftSession session, int fetchSize) {
        TGetTableTypesReq req = new TGetTableTypesReq(session.getSessionHandle());

        TGetTableTypesResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetTableTypes(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();

    }

    static ThriftOperation getDatabaseSchemaOperation(ThriftSession session, String catalog, String schemaPattern, int fetchSize) {
        TGetSchemasReq req = new TGetSchemasReq(session.getSessionHandle());
        req.setCatalogName(catalog);
        req.setSchemaName(schemaPattern);

        TGetSchemasResp resp;

        TCLIService.Iface client = session.getClient();

        try {
            resp = client.GetSchemas(req);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(resp.getStatus());

        return ThriftOperation.builder().handle(resp.getOperationHandle()).metaData(true).fetchSize(fetchSize).session(session).build();


    }

    private static void checkStatus(TStatus status) {

        TStatusCode statusCode = status.getStatusCode();

        if (statusCode == TStatusCode.SUCCESS_STATUS || statusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
            return;
        }

        throw new HiveThriftException(status);
    }

    public static TTableSchema getTableSchema(ThriftSession session, TOperationHandle handle) {
        TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(handle);

        TGetResultSetMetadataResp metadataResp;

        TCLIService.Iface client = session.getClient();

        try {
            metadataResp = client.GetResultSetMetadata(metadataReq);
        } catch (TException e) {
            throw new HiveException(e);
        }

        checkStatus(metadataResp.getStatus());

        return metadataResp.getSchema();
    }

    private static TRowSet getRowSet(ThriftSession session, TFetchResultsReq tFetchResultsReq) {
        TFetchResultsResp fetchResults;

        TCLIService.Iface client = session.getClient();

        try {
            fetchResults = client.FetchResults(tFetchResultsReq);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(fetchResults.getStatus());

        return fetchResults.getResults();
    }

    public static ThriftOperation executeSql(ThriftSession session, String sql, long queryTimeout, int fetchSize, int maxRows, int resultSetConcurrency, int resultSetHoldability, int resultSetType, int fetchDirection) {
        TExecuteStatementReq executeStatementReq = new TExecuteStatementReq(session.getSessionHandle(), StringUtils.trim(sql));
        executeStatementReq.setRunAsync(true);
        executeStatementReq.setQueryTimeout(queryTimeout);
        //todo: allows per statement configuration of session handle
        //executeStatementReq.setConfOverlay(null);

        TExecuteStatementResp executeStatementResp;

        TCLIService.Iface client = session.getClient();

        try {
            executeStatementResp = client.ExecuteStatement(executeStatementReq);
        } catch (TException e) {
            throw new HiveThriftException(e);
        }

        checkStatus(executeStatementResp.getStatus());

        TOperationHandle operationHandle = executeStatementResp.getOperationHandle();

        if (HiveDriverProperty.FETCH_SERVER_LOGS.getBoolean(session.getProperties())) {

            ExecutorService executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "fetch-logs-thread"));

            executorService.submit(() -> {
                List<Row> rows = fetchLogs(session, operationHandle, Schema.builder().descriptors(StaticColumnDescriptors.QUERY_LOG).build(), fetchSize);

                for (Row row : rows) {
                    try {
                        log.debug(row.getColumn(1).asString());
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });
        }


        waitForStatementToComplete(session, operationHandle);

        return ThriftOperation.builder()
                .session(session)
                .handle(operationHandle)
                .resultSetConcurrency(resultSetConcurrency)
                .resultSetHoldability(resultSetHoldability)
                .maxRows(maxRows)
                .fetchSize(fetchSize)
                .fetchDirection(fetchDirection)
                .resultSetType(resultSetType)
                .build();

    }

    private static void waitForStatementToComplete(ThriftSession session, TOperationHandle handle) {
        boolean isComplete = false;

        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(handle);

        while (!isComplete) {

            TGetOperationStatusResp statusResp;

            TCLIService.Iface client = session.getClient();

            try {
                statusResp = client.GetOperationStatus(statusReq);
            } catch (TException e) {
                throw new HiveThriftException(e);
            }

            checkStatus(statusResp.getStatus());

            if (statusResp.isSetOperationState()) {

                switch (statusResp.getOperationState()) {
                    case FINISHED_STATE:
                        isComplete = true;
                        break;
                    case CLOSED_STATE:
                    case CANCELED_STATE:
                    case TIMEDOUT_STATE:
                    case ERROR_STATE:
                    case UKNOWN_STATE:
                        throw new HiveThriftException(statusResp);
                    case INITIALIZED_STATE:
                    case PENDING_STATE:
                    case RUNNING_STATE:
                        break;
                }
            }


        }
    }

    private static List<Row> fetchResults(ThriftSession session, TOperationHandle operationHandle, TFetchOrientation orientation, int fetchSize, Schema schema) {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, orientation, fetchSize);
        fetchReq.setFetchType(FETCH_TYPE_QUERY);

        return getRows(session, schema, fetchReq);
    }

    private static List<Row> fetchLogs(ThriftSession session, TOperationHandle operationHandle, Schema schema, int fetchSize) {

        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, TFetchOrientation.FETCH_FIRST, fetchSize);
        fetchReq.setFetchType(FETCH_TYPE_LOG);

        return getRows(session, schema, fetchReq);

    }

    private static List<Row> getRows(ThriftSession session, Schema schema, TFetchResultsReq tFetchResultsReq) {
        TRowSet results = getRowSet(session, tFetchResultsReq);

        return getRows(results, schema);


    }

    private static List<Row> getRows(TRowSet rowSet, Schema schema) {

        if (rowSet != null && rowSet.isSetColumns()) {

            ColumnBasedSet cbs = ColumnBasedSet.builder().rowSet(rowSet).schema(schema).build();

            return RowBaseSet.builder().columnBaseSet(cbs).build().getRows();

        }

        return null;

    }


    public static Iterable<Row> getResults(ThriftSession session, TOperationHandle handle, int fetchSize, Schema schema) {
        return () -> {

            Iterator<List<Row>> fetchIterator = fetchIterator(session, handle, fetchSize, schema);

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

                List<Row> results = fetchResults(session, handle, TFetchOrientation.FETCH_NEXT, fetchSize, schema);

                if (results != null && !results.isEmpty()) {
                    return results;
                } else {
                    return endOfData();
                }

            }
        };
    }
}
