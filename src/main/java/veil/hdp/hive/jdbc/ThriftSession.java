package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ThriftSession implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    private final AtomicReference<TTransport> currentTransport = new AtomicReference<>();
    private final AtomicReference<TCLIService.Client> currentClient = new AtomicReference<>();
    private final AtomicReference<TSessionHandle> currentSession = new AtomicReference<>();
    private final AtomicReference<TProtocolVersion> currentProtocol = new AtomicReference<>();

    private final Properties properties;

    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final AtomicReference<CloseableHttpClient> httpClient = new AtomicReference<>();


    private ThriftSession(Properties properties, TTransport transport, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this.properties = properties;
        currentTransport.set(transport);
        currentClient.set(client);
        currentSession.set(sessionHandle);
        currentProtocol.set(protocolVersion);

        closed.set(false);
    }

    public TTransport getTransport() {
        return currentTransport.get();
    }

    public TCLIService.Client getClient() {
        return currentClient.get();
    }

    public TSessionHandle getSessionHandle() {
        return currentSession.get();
    }

    public TProtocolVersion getProtocolVersion() {
        return currentProtocol.get();
    }

    public boolean isClosed() {
        return closed.get();
    }


    public Properties getProperties() {
        return properties;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (log.isDebugEnabled()) {
                log.debug("attempting to close {}", this.getClass().getName());
            }

            HiveServiceUtils.closeSession(currentClient.get(), currentSession.get());
            ThriftUtils.closeTransport(currentTransport.get());

            currentProtocol.set(null);
            currentClient.set(null);
            currentSession.set(null);
            currentTransport.set(null);
        }
    }

    @Override
    public String toString() {
        return "ThriftSession{" +
                "currentTransport=" + currentTransport +
                ", currentClient=" + currentClient +
                ", currentSession=" + currentSession +
                ", currentProtocol=" + currentProtocol +
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

            TTransport transport;

            if (transportMode.equals(TransportMode.binary)) {
                transport = ThriftUtils.createBinaryTransport(properties, loginTimeout);
            } else {
                // todo: figure out how to close this
                CloseableHttpClient client = HttpUtils.buildClient(properties);
                transport = ThriftUtils.createHttpTransport(properties, client);

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
