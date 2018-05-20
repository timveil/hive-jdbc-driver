package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class ByteColumn extends AbstractColumn<Byte> {
    ByteColumn(Byte value) {
        super(value);
    }

    @Override
    public Byte getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Byte asByte() {
        return getValue();
    }

    @Override
    public String asString() {
        if (value != null) {
            return Byte.toString(value);
        }

        return null;
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
    public Short asShort() {
        return getValue().shortValue();
    }
}
