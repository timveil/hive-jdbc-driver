package veil.hdp.hive.jdbc;

import java.sql.SQLException;

public interface Closeable {
    void close() throws SQLException;
}
