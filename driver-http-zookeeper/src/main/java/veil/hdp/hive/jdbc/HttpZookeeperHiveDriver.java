package veil.hdp.hive.jdbc;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.*;
import veil.hdp.hive.jdbc.core.thrift.ThriftTransport;
import veil.hdp.hive.jdbc.http.HttpUtils;
import veil.hdp.hive.jdbc.zk.ZookeeperUtils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class HttpZookeeperHiveDriver extends HiveDriver {

    private static final Logger log = LoggerFactory.getLogger(HttpZookeeperHiveDriver.class);

    static {
        try {

            DriverManager.registerDriver(new HttpZookeeperHiveDriver());

            if (log.isInfoEnabled()) {
                log.info("driver [{}] has been registered.", HttpZookeeperHiveDriver.class.getName());
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }


    @Override
    ThriftTransport buildTransport(Properties properties) throws SQLException {

        CloseableHttpClient client = HttpUtils.buildClient(properties);

        TTransport httpTransport = HttpUtils.createHttpTransport(properties, client);

        return ThriftTransport.builder().transport(httpTransport).addCloseable(client).build();

    }

    @Override
    DefaultDriverProperties defaultDriverProperties() {
        return (properties) -> {
            HiveDriverProperty.TRANSPORT_MODE.set(properties, TransportMode.http.name());
            HiveDriverProperty.ZOOKEEPER_DISCOVERY_ENABLED.set(properties, true);


        };
    }

    @Override
    ZookeeperDiscoveryProperties zookeeperDiscoveryProperties() {
        return (uri, properties) -> {
            String authority = uri.getAuthority();

            ZookeeperUtils.loadPropertiesFromZookeeper(authority, properties);
        };
    }

    @Override
    UriProperties uriProperties() {
        return null;
    }
}
