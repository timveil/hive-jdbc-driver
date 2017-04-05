package veil.hdp.hive.jdbc;

import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

public interface PropertiesCallback {

    void merge(Properties properties, URI uri) throws SQLException;
}
