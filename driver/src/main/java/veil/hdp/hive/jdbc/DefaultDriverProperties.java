package veil.hdp.hive.jdbc;

import java.util.Properties;

@FunctionalInterface
public interface DefaultDriverProperties {
    void load(Properties properties);
}
