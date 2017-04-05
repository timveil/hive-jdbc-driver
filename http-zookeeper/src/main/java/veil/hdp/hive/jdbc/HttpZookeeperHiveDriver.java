package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.ZookeeperUtils;

import java.sql.DriverManager;
import java.sql.SQLException;

public class HttpZookeeperHiveDriver extends HttpHiveDriver {

    private static final Logger log = LoggerFactory.getLogger(HttpHiveDriver.class);

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
    PropertiesCallback buildPropertiesCallback() throws SQLException {
        return (properties, uri) -> {
            String authority = uri.getAuthority();

            ZookeeperUtils.loadPropertiesFromZookeeper(authority, properties);
        };
    }
}
