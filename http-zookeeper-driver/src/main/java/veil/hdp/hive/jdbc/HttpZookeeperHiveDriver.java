package veil.hdp.hive.jdbc;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HttpUtils;
import veil.hdp.hive.jdbc.utils.ZookeeperUtils;

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
    TTransport buildTransport(Properties properties) throws SQLException {

        CloseableHttpClient client = HttpUtils.buildClient(properties);

        return HttpUtils.createHttpTransport(properties, client);

    }

    @Override
    PropertiesCallback buildPropertiesCallback() throws SQLException {
        return (properties, uri) -> {
            HiveDriverProperty.TRANSPORT_MODE.set(properties, TransportMode.http.toString());
            HiveDriverProperty.ZOOKEEPER_DISCOVERY_ENABLED.set(properties, true);

            String authority = uri.getAuthority();

            ZookeeperUtils.loadPropertiesFromZookeeper(authority, properties);
        };
    }
}
