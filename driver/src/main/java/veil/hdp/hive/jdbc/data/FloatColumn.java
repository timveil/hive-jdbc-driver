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
    public Float asFloat() {
        return getValue();
    }

    @Override
    public String asString() {
        return Float.toString(getValue());
    }

    @Override
    public Integer asInt() {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Long asLong() {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Long.class, value);

        return getValue().longValue();
    }

    @Override
    public Double asDouble() {
        return getValue().doubleValue();
    }

    @Override
    public Short asShort() {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", Float.class, Byte.class, value);

        return getValue().byteValue();
    }
}
