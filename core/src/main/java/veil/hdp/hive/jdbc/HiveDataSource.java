package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.core.AbstractDataSource;
import veil.hdp.hive.jdbc.core.HiveDriverProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class HiveDataSource extends AbstractDataSource {

    private static final Logger log = LoggerFactory.getLogger(HiveDataSource.class);

    private String url;
    private Properties properties;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    abstract HiveDriver builDriver();

    @Override
    public Connection getConnection() throws SQLException {

        HiveDriver driver = builDriver();

        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {

        HiveDriver driver = builDriver();

        HiveDriverProperty.USER.set(properties, username);
        HiveDriverProperty.PASSWORD.set(properties, password);

        return driver.connect(url, properties);
    }




}
