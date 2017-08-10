package veil.hdp.hive.jdbc.thrift;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.TransportMode;
import veil.hdp.hive.jdbc.utils.BinaryUtils;
import veil.hdp.hive.jdbc.utils.HttpUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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


        private Properties properties;

        private ThriftTransportBuilder() {
        }

        public ThriftTransportBuilder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public ThriftTransport build() {

            TransportMode mode = TransportMode.valueOf(HiveDriverProperty.TRANSPORT_MODE.get(properties));

            TTransport transport = null;

            List<Closeable> closeableList = new ArrayList<>();

            if (mode.equals(TransportMode.binary)) {
                transport = BinaryUtils.createBinaryTransport(properties);
            } else if (mode.equals(TransportMode.http)) {
                CloseableHttpClient client = HttpUtils.buildClient(properties);

                closeableList.add(client);

                transport = HttpUtils.createHttpTransport(properties, client);
            }


            return new ThriftTransport(transport, closeableList);
        }


    }
}
