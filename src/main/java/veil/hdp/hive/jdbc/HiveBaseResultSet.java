package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.data.Row;
import veil.hdp.hive.jdbc.metadata.Schema;
import veil.hdp.hive.jdbc.utils.ResultSetUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by tveil on 4/5/17.
 */
public class HiveBaseResultSet extends AbstractResultSet {

    // constructor
    protected final Schema schema;


    // atomic
    protected final AtomicBoolean closed = new AtomicBoolean(true);
    protected final AtomicBoolean lastColumnNull = new AtomicBoolean(true);
    protected final AtomicInteger rowCount = new AtomicInteger(0);
    protected final AtomicReference<Row> currentRow = new AtomicReference<>();

    // public getter & setter
    protected SQLWarning sqlWarning = null;

    HiveBaseResultSet(Schema schema) {
        this.schema = schema;

        closed.set(false);
    }

    private <T> T checkValue(T value) {
        if (value == null) {
            lastColumnNull.set(true);
        } else {
            lastColumnNull.set(false);
        }

        return value;
    }


    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return ResultSetUtils.findColumnIndex(schema, columnLabel);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastColumnNull.get();
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData.Builder().schema(schema).build();
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }



    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asBigDecimal());
        }

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asBoolean());
        }

        return false;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asDate());
        }

        return null;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asDouble());
        }

        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }



    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asFloat());
        }

        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asInt());
        }

        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asLong());
        }

        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asObject());
        }

        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asShort());
        }

        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }


    @Override
    public String getString(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asString());
        }

        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asTimestamp());
        }

        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }


    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asByte());
        }

        return 0;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }



    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(checkValue(row.getColumn(columnIndex).asByteArray()));
        }

        return null;
    }


    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asInputStream());
        }

        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }


    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Row row = currentRow.get();

        if (row != null) {
            return checkValue(row.getColumn(columnIndex).asTime());
        }

        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }


}
