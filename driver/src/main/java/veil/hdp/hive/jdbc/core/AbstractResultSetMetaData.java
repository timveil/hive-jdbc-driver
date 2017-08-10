package veil.hdp.hive.jdbc.core;

import veil.hdp.hive.jdbc.HiveDriver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

abstract class AbstractResultSetMetaData implements ResultSetMetaData {
    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getColumnCount() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int isNullable(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getScale(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
