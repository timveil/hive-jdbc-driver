package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftSession implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    private final TTransport transport;
    private final TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private final TProtocolVersion protocolVersion;
    private final Properties properties;

    private final AtomicBoolean closed = new AtomicBoolean(true);


    private ThriftSession(Properties properties, TTransport transport, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this.properties = properties;
        this.transport = transport;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocolVersion = protocolVersion;

        closed.set(false);
    }

    public TTransport getTransport() {
        return transport;
    }

    public TCLIService.Client getClient() {
        return client;
    }

    public TSessionHandle getSessionHandle() {
        return sessionHandle;
    }

    public TProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public boolean isClosed() {
        return closed.get();
    }


    public Properties getProperties() {
        return properties;
    }

    @Override
    public void close() throws SQLException {
        if (closed.compareAndSet(false, true)) {
            HiveServiceUtils.closeSession(client, sessionHandle);
            ThriftUtils.closeTransport(transport);
        }
    }

    @Override
    public String toString() {
        return "ThriftSession{" +
                "transport=" + transport +
                ", client=" + client +
                ", sessionHandle=" + sessionHandle +
                ", protocolVersion=" + protocolVersion +
                ", properties=" + properties +
                '}';
    }

    public static class Builder {
        private Properties properties;
        private int loginTimeout;


        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder timeout(int loginTimeout) {
            this.loginTimeout = loginTimeout;
            return this;
        }


        public ThriftSession build() throws SQLException {

            TransportMode transportMode = TransportMode.valueOf(HiveDriverProperty.TRANSPORT_MODE.get(properties));

            TTransport transport = null;

            if (transportMode.equals(TransportMode.binary)) {
                transport = ThriftUtils.createBinaryTransport(properties, loginTimeout);
            } else {

                // todo: does this really need to be closed here
                try (CloseableHttpClient httpClient = HttpUtils.buildClient(properties)) {
                    transport = ThriftUtils.createHttpTransport(properties, httpClient);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }


            ThriftUtils.openTransport(transport);

            TCLIService.Client client = ThriftUtils.createClient(transport);

            TOpenSessionResp openSessionResp = HiveServiceUtils.openSession(properties, client);

            TProtocolVersion protocolVersion = openSessionResp.getServerProtocolVersion();

            TSessionHandle sessionHandle = openSessionResp.getSessionHandle();

            return new ThriftSession(properties, transport, client, sessionHandle, protocolVersion);
        }

    }
}
