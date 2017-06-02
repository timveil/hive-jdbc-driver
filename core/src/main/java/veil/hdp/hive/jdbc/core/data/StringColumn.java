package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static java.lang.Boolean.valueOf;

public class StringColumn extends BaseColumn<String> {

    StringColumn(ColumnDescriptor descriptor, String value) {
        super(descriptor, value);
    }

    @Override
    public String asString() throws SQLException {
        return value;
    }

    @Override
    public Boolean asBoolean() throws SQLException {
        if (value != null) {
            return valueOf(value);
        }

        return null;
    }

    @Override
    public Date asDate() throws SQLException {
        if (value != null) {
            return Date.valueOf(value);
        }

        return null;
    }

    @Override
    public Timestamp asTimestamp() throws SQLException {
        if (value != null) {
            return Timestamp.valueOf(value);
        }

        return null;
    }

    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        if (value != null) {
            return new BigDecimal(value);
        }

        return null;
    }

    @Override
    public Double asDouble() throws SQLException {
        if (value != null) {
            return Double.valueOf(value);
        }

        return null;
    }

    @Override
    public Float asFloat() throws SQLException {
        if (value != null) {
            return Float.valueOf(value);
        }

        return null;
    }

    @Override
    public Integer asInt() throws SQLException {
        if (value != null) {
            return Integer.valueOf(value);
        }

        return null;
    }

    @Override
    public Long asLong() throws SQLException {
        if (value != null) {
            return Long.valueOf(value);
        }

        return null;
    }

    @Override
    public Short asShort() throws SQLException {
        if (value != null) {
            return Short.valueOf(value);
        }

        return null;
    }

    @Override
    public Byte asByte() throws SQLException {
        if (value != null) {
            return Byte.valueOf(value);
        }

        return null;
    }

    @Override
    public Time asTime() throws SQLException {
        if (value != null) {
            return Time.valueOf(value);
        }

        return null;
    }

    @Override
    public InputStream asInputStream() throws SQLException {
        if (value != null) {
            return new ByteArrayInputStream(value.getBytes());
        }

        return null;
    }

    @Override
    public byte[] asByteArray() throws SQLException {
        if (value != null) {
            return value.getBytes();
        }

        return null;
    }

    @Override
    public Character asCharacter() throws SQLException {
        if (value != null) {

            if (value.length() != 1) {
                log.warn("may lose precision going from {} to {}; value [{}]", String.class, Character.class, value);
            }

            return value.charAt(0);
        }

        return null;
    }
}
