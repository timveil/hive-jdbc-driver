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

package veil.hdp.hive.jdbc.thrift;


import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.*;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.StaticColumnDescriptors;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftSession implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(ThriftSession.class);
    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);

    // constructor
    private final Properties properties;
    private final ThriftTransport thriftTransport;
    private final TCLIService.Iface client;
    private final TSessionHandle sessionHandle;


    private ThriftSession(Properties properties, ThriftTransport thriftTransport, TCLIService.Iface client, TSessionHandle sessionHandle) {
        this.properties = properties;
        this.thriftTransport = thriftTransport;
        this.client = client;
        this.sessionHandle = sessionHandle;

        closed.set(false);
    }

    public static ThriftSessionBuilder builder() {
        return new ThriftSessionBuilder();
    }

    public Properties getProperties() {
        return properties;
    }

    /**
     * Determines if the ThriftSession is in a valid state to execute another Thrift call. It checks both the closed flag as well as the underlying thrift transport status.
     *
     * @return true if valid, false if not valid
     */
    public boolean isValid() {
        return !closed.get() && thriftTransport.isValid();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            log.trace("attempting to close {}", this.getClass().getName());

            try {
                closeSession();

                if (!thriftTransport.isClosed()) {
                    thriftTransport.close();
                }

            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }
    }


    private void closeSession() {

        try {

            TCloseSessionReq closeRequest = new TCloseSessionReq(sessionHandle);

            TCloseSessionResp resp = client.CloseSession(closeRequest);


            if (resp != null) {
                try {
                    ThriftUtils.checkStatus(resp.getStatus());
                } catch (HiveThriftException e) {
                    log.warn(MessageFormat.format("sql exception: message [{0}]", e.getMessage()), e);
                }
            }

        } catch (TTransportException e) {
            log.warn(MessageFormat.format("thrift transport exception: type [{0}]", e.getType()), e);
        } catch (TException e) {
            log.warn(MessageFormat.format("thrift exception exception: message [{0}]", e.getMessage()), e);
        }


    }


    public ResultSet getCatalogs() {

        try {

            TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle);

            TGetCatalogsResp resp = client.GetCatalogs(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }

    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {

        try {

            TGetColumnsReq req = new TGetColumnsReq(sessionHandle);
            req.setCatalogName(catalog);
            req.setSchemaName(schemaPattern);
            req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);
            req.setColumnName(columnNamePattern == null ? "%" : columnNamePattern);

            TGetColumnsResp resp = client.GetColumns(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();


        } catch (TException e) {
            throw new HiveThriftException("error getting columns for catalog " + catalog + ", schema pattern " + schemaPattern + ", table name pattern " + tableNamePattern + ", column name pattern " + columnNamePattern, e);
        }

    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {

        try {

            TGetFunctionsReq req = new TGetFunctionsReq();
            req.setSessionHandle(sessionHandle);
            req.setCatalogName(catalog);
            req.setSchemaName(schemaPattern);
            req.setFunctionName(functionNamePattern == null ? "%" : functionNamePattern);

            TGetFunctionsResp resp = client.GetFunctions(req);


            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }

    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {

        try {
            TGetTablesReq req = new TGetTablesReq(sessionHandle);

            req.setCatalogName(catalog);
            req.setSchemaName(schemaPattern);
            req.setTableName(tableNamePattern == null ? "%" : tableNamePattern);

            if (types != null) {
                req.setTableTypes(Arrays.asList(types));
            }

            TGetTablesResp resp = client.GetTables(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }

    }

    public ResultSet getTypeInfo() {

        try {

            TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle);

            TGetTypeInfoResp resp = client.GetTypeInfo(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }


    }

    public TGetInfoValue getServerInfo(TGetInfoType type) {

        try {

            TGetInfoReq req = new TGetInfoReq(sessionHandle, type);

            TGetInfoResp resp = client.GetInfo(req);

            ThriftUtils.checkStatus(resp.getStatus());

            return resp.getInfoValue();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }


    }

    public ResultSet getTableTypes() {

        try {
            TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle);

            TGetTableTypesResp resp = client.GetTableTypes(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }

    }

    public ResultSet getSchemas(String catalog, String schemaPattern) {

        try {
            TGetSchemasReq req = new TGetSchemasReq(sessionHandle);
            req.setCatalogName(catalog);
            req.setSchemaName(schemaPattern);

            TGetSchemasResp resp = client.GetSchemas(req);

            ThriftUtils.checkStatus(resp.getStatus());

            ThriftOperation thriftOperation = ThriftOperation.builder().handle(resp.getOperationHandle()).client(client).build();

            return HiveResultSet.builder().thriftOperation(thriftOperation).fetchSize(HiveDriverProperty.FETCH_SIZE.getInt(properties)).build();

        } catch (TException e) {
            throw new HiveThriftException(e);
        }

    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PRIMARY_KEYS).build()).build();
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PROCEDURE_COLUMNS).build()).build();
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PROCEDURES).build()).build();
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.COLUMN_PRIVILEGES).build()).build();
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.TABLE_PRIVILEGES).build()).build();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.BEST_ROW_IDENTIFIER).build()).build();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.VERSION_COLUMNS).build()).build();
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.IMPORTED_KEYS).build()).build();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.EXPORTED_KEYS).build()).build();
    }

    //TODO - NOW AVAILABLE
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.CROSS_REFERENCE).build()).build();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.INDEX_INFO).build()).build();
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.UDT).build()).build();
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.SUPER_TYPES).build()).build();
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.SUPER_TABLES).build()).build();
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.ATTRIBUTES).build()).build();
    }

    public ResultSet getClientInfoProperties() {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.CLIENT_INFO_PROPERTIES).build()).build();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.FUNCTION_COLUMNS).build()).build();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.PSEUDO_COLUMNS).build()).build();
    }

    public ResultSet getGeneratedKeys() {
        return HiveEmptyResultSet.builder().schema(Schema.builder().descriptors(StaticColumnDescriptors.GENERATED_KEYS).build()).build();
    }


    public ThriftOperation executeSql(String sql, long queryTimeout) {
        TExecuteStatementReq executeStatementReq = new TExecuteStatementReq(sessionHandle, StringUtils.trim(sql));
        executeStatementReq.setRunAsync(true);
        executeStatementReq.setQueryTimeout(queryTimeout);
        //todo: allows per statement configuration of session handle
        //executeStatementReq.setConfOverlay(null);

        TExecuteStatementResp executeStatementResp;

        try {
            executeStatementResp = client.ExecuteStatement(executeStatementReq);
        } catch (TException e) {
            throw new HiveThriftException("error executing sql [" + sql + ']', e);
        }

        ThriftUtils.checkStatus(executeStatementResp.getStatus());

        TOperationHandle operationHandle = executeStatementResp.getOperationHandle();

        /*
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
        */

        waitForStatementToComplete(operationHandle);

        return ThriftOperation.builder()
                .client(client)
                .handle(operationHandle)
                .build();

    }

    private void waitForStatementToComplete(TOperationHandle handle) {
        boolean isComplete = false;

        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(handle);

        while (!isComplete) {

            TGetOperationStatusResp statusResp;

            try {
                statusResp = client.GetOperationStatus(statusReq);
            } catch (TException e) {
                throw new HiveThriftException("error checking status for [" + handle + ']', e);
            }

            ThriftUtils.checkStatus(statusResp.getStatus());

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


    public static class ThriftSessionBuilder implements Builder<ThriftSession> {
        private Properties properties;

        private ThriftSessionBuilder() {
        }

        public ThriftSessionBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }


        @Override
        public ThriftSession build() {

            ThriftTransport thriftTransport = null;

            int protocol = HiveDriverProperty.THRIFT_PROTOCOL_VERSION.getInt(properties);

            while (protocol >= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8.getValue()) {

                TProtocolVersion protocolVersion = TProtocolVersion.findByValue(protocol);

                log.debug("trying protocol {}", protocolVersion);

                try {

                    thriftTransport = ThriftTransport.builder().properties(properties).build();

                    TCLIService.Iface client = ThriftUtils.createClient(thriftTransport);

                    TOpenSessionResp openSessionResp = ThriftUtils.openSession(properties, client, protocolVersion);

                    TSessionHandle sessionHandle = openSessionResp.getSessionHandle();

                    TProtocolVersion serverProtocolVersion = openSessionResp.getServerProtocolVersion();

                    log.debug("opened session with protocol {}", serverProtocolVersion);

                    return new ThriftSession(properties, thriftTransport, client, sessionHandle);

                } catch (InvalidProtocolException e) {
                    protocol--;

                    try {
                        thriftTransport.close();
                    } catch (Exception io) {
                        log.warn(io.getMessage(), io);
                    }
                }
            }

            throw new HiveException("cannot build ThriftSession.  check that the thrift protocol version on the server is compatible with this driver.");
        }

    }

}
