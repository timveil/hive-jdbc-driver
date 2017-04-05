package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.math.BigDecimal;
import java.sql.SQLException;

public class DecimalColumn extends AbstractColumn<BigDecimal> {
    public DecimalColumn(ColumnDescriptor descriptor, BigDecimal value) {
        super(descriptor, value);
    }

    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return value.toString();
        }
        return null;
    }


    @Override
    public Integer asInt() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Integer.class, value);

        if (value != null) {
            return value.intValue();
        }
        return null;
    }

    @Override
    public Long asLong() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Long.class, value);

        if (value != null) {
            return value.longValue();
        }
        return null;
    }

    @Override
    public Double asDouble() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Double.class, value);

        if (value != null) {
            return value.doubleValue();
        }
        return null;
    }

    @Override
    public Float asFloat() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Float.class, value);

        if (value != null) {
            return value.floatValue();
        }
        return null;
    }

    @Override
    public Short asShort() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Short.class, value);

        if (value != null) {
            return value.shortValue();
        }
        return null;
    }


    @Override
    public Byte asByte() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Byte.class, value);

        if (value != null) {
            return value.byteValue();
        }
        return null;
    }
}
