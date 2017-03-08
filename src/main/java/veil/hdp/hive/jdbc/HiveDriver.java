package veil.hdp.hive.jdbc;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.UrlUtils;

import javax.security.sasl.SaslException;
import java.sql.*;
import java.util.Properties;

public class HiveDriver extends AbstractHiveDriver {

    private static final Logger log = LoggerFactory.getLogger(HiveDriver.class);

    static {
        try {
            java.sql.DriverManager.registerDriver(new HiveDriver());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        try {
            return acceptsURL(url) ? new HiveConnection(url, info) : null;
        } catch (TException | SaslException e) {
            throw new SQLException(e);
        }
    }

    public boolean acceptsURL(String url) throws SQLException {
        return UrlUtils.acceptURL(url);
    }


}
