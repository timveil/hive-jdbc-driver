package veil.hdp.hive.jdbc;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HttpUtils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class HttpHiveDriver extends HiveDriver {

    private static final Logger log = LoggerFactory.getLogger(HttpHiveDriver.class);

    static {
        try {

            DriverManager.registerDriver(new HttpHiveDriver());

            if (log.isInfoEnabled()) {
                log.info("driver [{}] has been registered.", HttpHiveDriver.class.getName());
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
    PropertiesCallback buildPropertiesCallback() {
        return (properties, uri) -> {
            HiveDriverProperty.TRANSPORT_MODE.set(properties, TransportMode.http.name());

            HiveDriverProperty.HOST_NAME.set(properties, uri.getHost());

            if (uri.getPort() != -1) {
                HiveDriverProperty.PORT_NUMBER.set(properties, uri.getPort());
            }
        };
    }
}
