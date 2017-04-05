package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class LongColumn extends AbstractColumn<Long> {
    public LongColumn(ColumnDescriptor descriptor, Long value) {
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
    public String asString() throws SQLException {
        return Long.toString(getValue());
    }


    @Override
    public Float asFloat() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Float.class, value);

        return getValue().floatValue();
    }


    @Override
    public Integer asInt() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Double asDouble() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Double.class, value);

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
