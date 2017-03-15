package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.DriverUtils;

import java.sql.*;
import java.util.Properties;

public class HiveDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    static {
        try {
            java.sql.DriverManager.registerDriver(new HiveDriver());

            log.debug("driver has been registered");

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }


    private Connection connect(Properties properties) throws SQLException {
        HiveConnection connection = new HiveConnection(properties);
        connection.connect();

        return connection;
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            return connect(DriverUtils.buildProperties(url, info));
        }

        return null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverUtils.buildDriverPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean acceptsURL(String url) throws SQLException {

        if (url == null) {
            throw new SQLException("url is null");
        }

        return DriverUtils.acceptURL(url);
    }



}
