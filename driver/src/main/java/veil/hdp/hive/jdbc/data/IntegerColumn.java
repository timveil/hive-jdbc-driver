package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class IntegerColumn extends AbstractColumn<Integer> {
    IntegerColumn(Integer value) {
        super(value);
    }

    @Override
    public Integer getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Integer asInt() {
        return getValue();
    }


    @Override
    public Boolean asBoolean() {
        if (value != null) {
            return value == 1;
        }

        return null;
    }


    @Override
    public String asString() {
        return Integer.toString(getValue());
    }


    @Override
    public Float asFloat() {
        return getValue().floatValue();
    }

    @Override
    public Long asLong() {
        return getValue().longValue();
    }

    @Override
    public Double asDouble() {
        return getValue().doubleValue();
    }

    @Override
    public Short asShort() {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", Integer.class, Byte.class, value);

        return getValue().byteValue();
    }
}
