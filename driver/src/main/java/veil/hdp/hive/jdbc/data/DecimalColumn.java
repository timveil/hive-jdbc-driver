package veil.hdp.hive.jdbc.data;

import java.math.BigDecimal;
import java.sql.SQLException;

public class DecimalColumn extends AbstractColumn<BigDecimal> {
    DecimalColumn(BigDecimal value) {
        super(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public String asString() {
        if (value != null) {
            return value.toString();
        }
        return null;
    }


    @Override
    public Integer asInt() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Integer.class, value);

        if (value != null) {
            return value.intValue();
        }
        return null;
    }

    @Override
    public Long asLong() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Long.class, value);

        if (value != null) {
            return value.longValue();
        }
        return null;
    }

    @Override
    public Double asDouble() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Double.class, value);

        if (value != null) {
            return value.doubleValue();
        }
        return null;
    }

    @Override
    public Float asFloat() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Float.class, value);

        if (value != null) {
            return value.floatValue();
        }
        return null;
    }

    @Override
    public Short asShort() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Short.class, value);

        if (value != null) {
            return value.shortValue();
        }
        return null;
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Byte.class, value);

        if (value != null) {
            return value.byteValue();
        }
        return null;
    }
}
