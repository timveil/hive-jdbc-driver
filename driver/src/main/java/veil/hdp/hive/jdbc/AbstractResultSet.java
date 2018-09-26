/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

abstract class AbstractResultSet implements ResultSet {
    @Override
    public final BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNull(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateByte(int columnIndex, byte x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateShort(int columnIndex, short x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateInt(int columnIndex, int x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateLong(int columnIndex, long x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateFloat(int columnIndex, float x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateDouble(int columnIndex, double x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateString(int columnIndex, String x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateDate(int columnIndex, Date x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateTime(int columnIndex, Time x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(int columnIndex, Object x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNull(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateByte(String columnLabel, byte x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateShort(String columnLabel, short x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateInt(String columnLabel, int x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateLong(String columnLabel, long x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateFloat(String columnLabel, float x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateDouble(String columnLabel, double x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateString(String columnLabel, String x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateDate(String columnLabel, Date x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateTime(String columnLabel, Time x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(String columnLabel, Object x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateRef(int columnIndex, Ref x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateRef(String columnLabel, Ref x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(int columnIndex, Clob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(String columnLabel, Clob x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateArray(int columnIndex, Array x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateArray(String columnLabel, Array x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNString(int columnIndex, String nString) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNString(String columnLabel, String nString) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final String getCursorName() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Reader getCharacterStream(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Reader getCharacterStream(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Blob getBlob(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Clob getClob(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Blob getBlob(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Clob getClob(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final NClob getNClob(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final NClob getNClob(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final String getNString(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final String getNString(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Ref getRef(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Array getArray(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Ref getRef(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Array getArray(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final URL getURL(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final URL getURL(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final RowId getRowId(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final RowId getRowId(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isAfterLast() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isFirst() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isLast() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void beforeFirst() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void afterLast() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean first() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean last() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean absolute(int row) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean relative(int rows) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean previous() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean rowUpdated() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean rowInserted() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean rowDeleted() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void insertRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void deleteRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void refreshRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void cancelRowUpdates() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void moveToInsertRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void moveToCurrentRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean next() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void close() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getRow() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getType() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getHoldability() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
