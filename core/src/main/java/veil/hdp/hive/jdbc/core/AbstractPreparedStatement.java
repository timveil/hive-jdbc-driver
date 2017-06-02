package veil.hdp.hive.jdbc.core;

import veil.hdp.hive.jdbc.HiveDriver;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;


abstract class AbstractPreparedStatement extends HiveStatement implements PreparedStatement {


    AbstractPreparedStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void clearParameters() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean execute() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void addBatch() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
