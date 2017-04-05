package veil.hdp.hive.jdbc;

import java.sql.SQLException;

interface SQLCloseable {
    void close() throws SQLException;
}
