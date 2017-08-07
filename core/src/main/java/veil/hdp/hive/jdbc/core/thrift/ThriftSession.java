package veil.hdp.hive.jdbc.core.thrift;


import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.Builder;
import veil.hdp.hive.jdbc.core.binding.TCLIService;
import veil.hdp.hive.jdbc.core.binding.TOpenSessionResp;
import veil.hdp.hive.jdbc.core.binding.TProtocolVersion;
import veil.hdp.hive.jdbc.core.binding.TSessionHandle;
import veil.hdp.hive.jdbc.core.utils.ThriftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ThriftSession implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ThriftSession.class);

    // constructor
    private final ThriftTransport thriftTransport;
    private final TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private final TProtocolVersion protocolVersion;
    private final Properties properties;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private final ReentrantLock sessionLock = new ReentrantLock(true);


    private ThriftSession(Properties properties, ThriftTransport thriftTransport, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this.properties = properties;
        this.thriftTransport = thriftTransport;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocolVersion = protocolVersion;

        closed.set(false);
    }

    public static ThriftSessionBuilder builder() {
        return new ThriftSessionBuilder();
    }

    public TTransport getTransport() {
        return thriftTransport.getTransport();
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            ThriftUtils.closeSession(this);

            thriftTransport.close();
        }
    }

    public static class ThriftSessionBuilder implements Builder<ThriftSession> {
        private Properties properties;
        private ThriftTransport thriftTransport;

        private ThriftSessionBuilder() {
        }

        public ThriftSessionBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public ThriftSessionBuilder thriftTransport(ThriftTransport thriftTransport) {
            this.thriftTransport = thriftTransport;
            return this;
        }

        @Override
        public ThriftSession build() {

            TTransport transport = thriftTransport.getTransport();

            ThriftUtils.openTransport(transport, properties);

            TCLIService.Client client = ThriftUtils.createClient(transport);

            TOpenSessionResp openSessionResp = ThriftUtils.openSession(properties, client);

            TProtocolVersion protocolVersion = openSessionResp.getServerProtocolVersion();

            TSessionHandle sessionHandle = openSessionResp.getSessionHandle();

            return new ThriftSession(properties, thriftTransport, client, sessionHandle, protocolVersion);
        }

    }
}
