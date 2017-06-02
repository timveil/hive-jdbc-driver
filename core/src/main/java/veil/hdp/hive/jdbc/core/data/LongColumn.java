package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public class LongColumn extends BaseColumn<Long> {
    LongColumn(ColumnDescriptor descriptor, Long value) {
        super(descriptor, value);
    }

    @Override
    public Long getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Long asLong() throws SQLException {
        return getValue();
    }

    @Override
    public Boolean asBoolean() throws SQLException {
        if (value != null) {
            return value == 1;
        }

        return null;
    }

    @Override
    public Date asDate() throws SQLException {
        if (value != null) {
            return new Date(value);
        }

        return null;
    }

    @Override
    public Timestamp asTimestamp() throws SQLException {
        if (value != null) {
            return new Timestamp(value);
        }

        return null;
    }

    @Override
    public Time asTime() throws SQLException {
        if (value != null) {
            return new Time(value);
        }

        return null;
    }

    @Override
    public String asString() throws SQLException {
        return Long.toString(getValue());
    }


    @Override
    public Float asFloat() throws SQLException {
        return getValue().floatValue();
    }


    @Override
    public Integer asInt() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Double asDouble() throws SQLException {
        return getValue().doubleValue();
    }

    @Override
    public Short asShort() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Byte.class, value);

        return getValue().byteValue();
    }
}
