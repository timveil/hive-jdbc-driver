package veil.hdp.hive.jdbc;

import java.sql.SQLException;

public interface SQLCloseable {
    void close() throws SQLException;
}
