package veil.hdp.hive.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

class AbstractParameterMetaData implements ParameterMetaData {
    @Override
    public int getParameterCount() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
