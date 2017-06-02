package veil.hdp.hive.jdbc.core.thrift;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.Builder;
import veil.hdp.hive.jdbc.core.utils.ThriftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThriftTransport implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ThriftTransport.class);

    private final TTransport transport;

    private final List<Closeable> closeableList;

    private ThriftTransport(TTransport transport, List<Closeable> closeableList) {
        this.transport = transport;
        this.closeableList = closeableList;
    }

    public static ThriftTransportBuilder builder() {
        return new ThriftTransportBuilder();
    }

    public TTransport getTransport() {
        return transport;
    }

    @Override
    public void close() throws IOException {
        ThriftUtils.closeTransport(transport);

        for (Closeable closeable : closeableList) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.warn(e.getMessage(), e);
            }
        }
    }

    public static class ThriftTransportBuilder implements Builder<ThriftTransport> {


        private TTransport transport;

        private List<Closeable> closeableList = new ArrayList<>();

        private ThriftTransportBuilder() {
        }

        public ThriftTransportBuilder transport(TTransport transport) {
            this.transport = transport;
            return this;
        }

        public ThriftTransportBuilder addCloseable(Closeable closeable) {
            closeableList.add(closeable);
            return this;
        }

        public ThriftTransport build() {
            return new ThriftTransport(transport, closeableList);
        }


    }
}
