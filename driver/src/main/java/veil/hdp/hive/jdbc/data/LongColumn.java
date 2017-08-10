package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class LongColumn extends BaseColumn<Long> {
            /*
    resist temptation to convert long to timestamp/date/time.  java.sql.date does not need/want time and doesn't fit spec
    furthermore, time as long assumes timezone as GMT and can cause unexpected results.
    for hive always use string representation and static helpers on java.sql.Date to construct
     */


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
