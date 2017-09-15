package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class FloatColumn extends AbstractColumn<Float> {
    FloatColumn(Float value) {
        super(value);
    }

    @Override
    public Float getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Float asFloat() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        return Float.toString(getValue());
    }

    @Override
    public Integer asInt() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Long asLong() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Long.class, value);

        return getValue().longValue();
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
