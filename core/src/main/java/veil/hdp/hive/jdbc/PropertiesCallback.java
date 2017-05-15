package veil.hdp.hive.jdbc;

import java.net.URI;
import java.util.Properties;

public interface PropertiesCallback {

    void merge(Properties properties, URI uri);
}
