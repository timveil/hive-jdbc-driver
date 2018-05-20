package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class ShortColumn extends AbstractColumn<Short> {

    ShortColumn(Short value) {
        super(value);
    }

    @Override
    public Short getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Short asShort() {
        return getValue();
    }

    @Override
    public String asString() {
        return Short.toString(getValue());
    }

    @Override
    public Boolean asBoolean() {
        if (value != null) {
            return value == 1;
        }

        return null;
    }

    @Override
    public Float asFloat() {
        return getValue().floatValue();
    }


    @Override
    public Integer asInt() {
        return getValue().intValue();

    }

    @Override
    public Double asDouble() {
        return getValue().doubleValue();
    }

    @Override
    public Long asLong() {
        return getValue().longValue();
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", Short.class, Byte.class, value);

        return getValue().byteValue();
    }
}
