package veil.hdp.hive.jdbc.utils;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveThriftException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ThriftUtils {

    private static final Logger log = LoggerFactory.getLogger(ThriftUtils.class);

    public static void openTransport(TTransport transport) throws HiveThriftException {

        if (!transport.isOpen()) {
            try {
                transport.open();
            } catch (TTransportException e) {
                throw new HiveThriftException(e);
            }
        }

    }

    public static void closeTransport(TTransport transport) {

        if (transport.isOpen()) {
            transport.close();
        }

    }

    public static TCLIService.Client createClient(TTransport transport) {
        return new TCLIService.Client(new TBinaryProtocol(transport));
    }




    public static TOpenSessionResp openSession(Properties properties, TCLIService.Client client) throws HiveThriftException {
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        String username = HiveDriverProperty.USER.get(properties);

        if (username != null) {
            openSessionReq.setUsername(username);
            openSessionReq.setPassword(HiveDriverProperty.PASSWORD.get(properties));
        }

        // set properties for session
        Map<String, String> configuration = buildSessionConfig(properties);

        if (log.isTraceEnabled()) {
            log.trace("configuration for session provided to thrift {}", configuration);
        }

        openSessionReq.setConfiguration(configuration);

        if (log.isTraceEnabled()) {
            log.trace(openSessionReq.toString());
        }

        try {
            TOpenSessionResp resp = client.OpenSession(openSessionReq);

            QueryUtils.checkStatus(resp.getStatus());

            return resp;
        } catch (TException e) {
            throw new HiveThriftException(e);
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

}
