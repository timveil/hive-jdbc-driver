package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.ConnectionParameters;
import veil.hdp.hive.jdbc.OperationHandleCallback;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hive.service.cli.thrift.TCLIServiceConstants.TYPE_NAMES;

/**
 * Created by timve on 3/6/2017.
 */
public class HiveServiceUtils {

    private static final Logger log = LoggerFactory.getLogger(HiveServiceUtils.class);

    public static void verifySuccessWithInfo(TStatus status) throws SQLException {
        verifySuccess(status, true);
    }

    public static void verifySuccess(TStatus status) throws SQLException {
        verifySuccess(status, false);
    }

    public static void verifySuccess(TStatus status, boolean withInfo) throws SQLException {
        if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS || (withInfo && status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS)) {
            return;
        }

        throw new HiveSQLException(status);
    }

    public static TRowSet fetchResults(TCLIService.Client client, TOperationHandle operationHandle, TFetchOrientation orientation, int fetchSize) throws TException {
        TFetchResultsReq fetchReq = new TFetchResultsReq(operationHandle, orientation, fetchSize);
        TFetchResultsResp fetchResp = client.FetchResults(fetchReq);


        return fetchResp.getResults();
    }

    public static void closeOperation(TCLIService.Client client, TOperationHandle operationHandle) {
        TCloseOperationReq closeRequest = new TCloseOperationReq(operationHandle);

        try {
            client.CloseOperation(closeRequest);
        } catch (TException e) {
            log.warn(e.getMessage(), e);
        }
    }

    public static void cancelOperation(TCLIService.Client client, TOperationHandle operationHandle) {
        TCancelOperationReq cancelRequest = new TCancelOperationReq(operationHandle);

        try {
            client.CancelOperation(cancelRequest);
        } catch (TException e) {
            log.warn(e.getMessage(), e);
        }
    }

    public static void closeSession(TCLIService.Client client, TSessionHandle sessionHandle) {
        TCloseSessionReq closeRequest = new TCloseSessionReq(sessionHandle);

        try {
            client.CloseSession(closeRequest);
        } catch (TException e) {
            log.warn(e.getMessage(), e);
        }
    }

    public static TOperationHandle executeSql(TCLIService.Client client, TSessionHandle sessionHandle, long queryTimeout, String sql) throws TException, SQLException {
        TExecuteStatementReq executeStatementReq = new TExecuteStatementReq(sessionHandle, sql);
        executeStatementReq.setRunAsync(true);
        executeStatementReq.setQueryTimeout(queryTimeout);

        TExecuteStatementResp executeStatementResp = client.ExecuteStatement(executeStatementReq);

        return executeStatementResp.getOperationHandle();


    }

    public static void waitForStatementToComplete(TCLIService.Client client, TOperationHandle statementHandle) throws TException, SQLException {
        boolean isComplete = false;

        while (!isComplete) {

            TGetOperationStatusReq statusReq = new TGetOperationStatusReq(statementHandle);
            TGetOperationStatusResp statusResp = client.GetOperationStatus(statusReq);

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


    public static TSessionHandle openSession(ConnectionParameters connectionParameters, TCLIService.Client client) throws TException {
        TOpenSessionReq openSessionReq = new TOpenSessionReq();

        openSessionReq.setConfiguration(buildSessionConfig(connectionParameters));

        TOpenSessionResp openResp = client.OpenSession(openSessionReq);

        log.debug("status {}", openResp.getStatus());
        log.debug("protocol version {}", openResp.getServerProtocolVersion());

        return openResp.getSessionHandle();

    }


    private static Map<String, String> buildSessionConfig(ConnectionParameters connectionParameters) {
        Map<String, String> openSessionConfig = new HashMap<>();

        for (Map.Entry<String, String> hiveConf : connectionParameters.getHiveConfigurationParameters().entrySet()) {
            openSessionConfig.put("set:hiveconf:" + hiveConf.getKey(), hiveConf.getValue());
        }

        for (Map.Entry<String, String> hiveVar : connectionParameters.getHiveVariables().entrySet()) {
            openSessionConfig.put("set:hivevar:" + hiveVar.getKey(), hiveVar.getValue());
        }

        openSessionConfig.put("use:database", connectionParameters.getDatabaseName());

        Map<String, String> sessionVariables = connectionParameters.getSessionVariables();

        if (sessionVariables.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
            openSessionConfig.put(HiveAuthFactory.HS2_PROXY_USER, sessionVariables.get(HiveAuthFactory.HS2_PROXY_USER));
        }

        return openSessionConfig;
    }

    public static TTableSchema getSchema(TCLIService.Client client, TOperationHandle operationHandle) throws TException {

        TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(operationHandle);
        TGetResultSetMetadataResp metadataResp = client.GetResultSetMetadata(metadataReq);

        return metadataResp.getSchema();
    }
}
