package veil.hdp.hive.jdbc.thrift;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.metadata.ColumnTypeDescriptor;
import veil.hdp.hive.jdbc.utils.ThriftUtils;
import veil.hdp.hive.jdbc.utils.TypeDescriptorUtils;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftSession implements Closeable {

    private static final Logger log =  LogManager.getLogger(ThriftSession.class);

    // constructor
    private final ThriftTransport thriftTransport;
    private final TCLIService.Iface client;
    private final TSessionHandle sessionHandle;
    private final Properties properties;
    private final TProtocolVersion protocol;

    // atomic
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final LoadingCache<TTypeDesc, ColumnTypeDescriptor> cache = CacheBuilder.newBuilder()
            .maximumSize(50)
            .build(new CacheLoader<TTypeDesc, ColumnTypeDescriptor>() {
                @Override
                public ColumnTypeDescriptor load(@Nonnull TTypeDesc tTypeDesc) throws Exception {
                    return TypeDescriptorUtils.getDescriptor(tTypeDesc);
                }
            });


    private ThriftSession(Properties properties, ThriftTransport thriftTransport, TCLIService.Iface client, TSessionHandle sessionHandle, TProtocolVersion protocol) {
        this.properties = properties;
        this.thriftTransport = thriftTransport;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocol = protocol;

        closed.set(false);
    }

    public static ThriftSessionBuilder builder() {
        return new ThriftSessionBuilder();
    }

    public ColumnTypeDescriptor getCachedDescriptor(TTypeDesc type) {
        try {
            return cache.get(type);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public TCLIService.Iface getClient() {
        return client;
    }

    public TSessionHandle getSessionHandle() {
        return sessionHandle;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public Properties getProperties() {
        return properties;
    }

    public TProtocolVersion getProtocol() {
        return protocol;
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {

            if (log.isTraceEnabled()) {
                log.trace("attempting to close {}", this.getClass().getName());
            }

            ThriftUtils.closeSession(this);

            thriftTransport.close();

            cache.invalidateAll();

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

                    return new ThriftSession(properties, thriftTransport, client, sessionHandle, serverProtocolVersion);

                } catch (InvalidProtocolException e) {
                    protocol--;

                    try {
                        thriftTransport.close();
                    } catch (IOException io) {
                        log.warn(io.getMessage(), io);
                    }
                }
            }

            throw new HiveException("cannot build ThriftSession.  check that the thrift protocol version on the server is compatible with this driver.");
        }

    }
}
