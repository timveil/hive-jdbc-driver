package veil.hdp.hive.jdbc.core;

import java.util.Properties;

@FunctionalInterface
public interface DefaultDriverProperties {
    void load(Properties properties);
}
