package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
public class IntegerColumn extends AbstractColumn<Integer> {
    public IntegerColumn(ColumnDescriptor descriptor, Integer value) {
        super(descriptor, value);
    }

    @Override
    public Integer getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Integer asInt() throws SQLException {
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
        return Integer.toString(getValue());
    }


    @Override
    public Float asFloat() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Float.class, value);

        return getValue().floatValue();
    }

    @Override
    public Long asLong() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Long.class, value);

        return getValue().longValue();
    }

    @Override
    public Double asDouble() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Double.class, value);

        return getValue().doubleValue();
    }

    @Override
    public Short asShort() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Byte.class, value);

        return getValue().byteValue();
    }
}
