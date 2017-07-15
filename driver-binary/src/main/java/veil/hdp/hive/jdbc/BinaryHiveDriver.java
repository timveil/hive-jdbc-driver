package veil.hdp.hive.jdbc;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.binary.BinaryUtils;
import veil.hdp.hive.jdbc.core.*;
import veil.hdp.hive.jdbc.core.thrift.ThriftTransport;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryHiveDriver extends HiveDriver {

    private static final Logger log = LoggerFactory.getLogger(BinaryHiveDriver.class);

    static {
        try {

            DriverManager.registerDriver(new BinaryHiveDriver());

            if (log.isInfoEnabled()) {
                log.info("driver [{}] has been registered.", BinaryHiveDriver.class.getName());
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }


    @Override
    ThriftTransport buildTransport(Properties properties) throws SQLException {
        TTransport binaryTransport = BinaryUtils.createBinaryTransport(properties);

        return ThriftTransport.builder().transport(binaryTransport).build();
    }

    @Override
    DefaultDriverProperties defaultDriverProperties() {
        return (properties) -> {
            HiveDriverProperty.TRANSPORT_MODE.set(properties, TransportMode.binary.name());
        };
    }

    @Override
    UriProperties uriProperties() {
        return new UriProperties() {
            @Override
            public void load(URI uri, Properties properties) {

                HiveDriverProperty.HOST_NAME.set(properties, uri.getHost());

                if (uri.getPort() != -1) {
                    HiveDriverProperty.PORT_NUMBER.set(properties, uri.getPort());
                }
            }
        };
    }

    @Override
    ZookeeperDiscoveryProperties zookeeperDiscoveryProperties() {
        return null;
    }
}
