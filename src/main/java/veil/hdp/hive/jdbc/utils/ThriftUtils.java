package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.ConnectionParameters;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class ThriftUtils {

    private static final Logger log = LoggerFactory.getLogger(ThriftUtils.class);

    public static TSessionHandle openSession(ConnectionParameters connectionParameters, TCLIService.Client client) throws SQLException {
        TOpenSessionReq openSessionReq = new TOpenSessionReq();

        Map<String, String> openSessionConfig = buildSessionConfig(connectionParameters);

        openSessionReq.setConfiguration(openSessionConfig);

/*        // Store the user name in the open request in case no non-sasl authentication
        if (JdbcConnectionParams.AUTH_SIMPLE.equals(sessConfMap.get(JdbcConnectionParams.AUTH_TYPE))) {
            openSessionReq.setUsername(sessConfMap.get(JdbcConnectionParams.AUTH_USER));
            openSessionReq.setPassword(sessConfMap.get(JdbcConnectionParams.AUTH_PASSWD));
        }*/

        try {
            TOpenSessionResp openResp = client.OpenSession(openSessionReq);

            log.debug("status {}", openResp.getStatus());
            log.debug("protocol version {}", openResp.getServerProtocolVersion());

            return openResp.getSessionHandle();
        } catch (TException e) {
            throw new SQLException(e.getMessage(), e);
        }
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


    public static TTransport createBinaryTransport(ConnectionParameters connectionParameters, int loginTimeout) throws SQLException {

        if (!connectionParameters.isKerberos()) {
            return HiveAuthFactory.getSocketTransport(connectionParameters.getHost(), connectionParameters.getPort(), loginTimeout);
        }

        return null;
    }


}
