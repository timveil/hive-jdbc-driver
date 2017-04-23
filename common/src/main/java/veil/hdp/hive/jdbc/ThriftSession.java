package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.QueryUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ThriftSession implements SQLCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    // constructor
    private final TTransport transport;
    private final TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private final TProtocolVersion protocolVersion;
    private final Properties properties;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final ReentrantLock sessionLock = new ReentrantLock(true);


    private ThriftSession(Properties properties, TTransport transport, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this.properties = properties;
        this.transport = transport;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocolVersion = protocolVersion;

        closed.set(false);
    }

    public static ThriftSessionBuilder builder() {
        return new ThriftSessionBuilder();
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

    public ReentrantLock getSessionLock() {
        return sessionLock;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            QueryUtils.closeSession(this);
            ThriftUtils.closeTransport(transport);
        }
    }

    public static class ThriftSessionBuilder implements Builder<ThriftSession> {
        private Properties properties;
        private TTransport transport;

        private ThriftSessionBuilder() {
        }

        public ThriftSessionBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public ThriftSessionBuilder transport(TTransport transport) {
            this.transport = transport;
            return this;
        }

        @Override
        public ThriftSession build() {

            ThriftUtils.openTransport(transport);

            TCLIService.Client client = ThriftUtils.createClient(transport);

            TOpenSessionResp openSessionResp = ThriftUtils.openSession(properties, client);

            TProtocolVersion protocolVersion = openSessionResp.getServerProtocolVersion();

            TSessionHandle sessionHandle = openSessionResp.getSessionHandle();

            return new ThriftSession(properties, transport, client, sessionHandle, protocolVersion);
        }

    }
}
