package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
public class DoubleColumn extends AbstractColumn<Double> {
    public DoubleColumn(ColumnDescriptor descriptor, Double value) {
        super(descriptor, value);
    }

    @Override
    public Double getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Double asDouble() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        return Double.toString(getValue());
    }


    @Override
    public Integer asInt() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Long asLong() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Long.class, value);

        return getValue().longValue();
    }


    @Override
    public Float asFloat() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Float.class, value);

        return getValue().floatValue();
    }

    @Override
    public Short asShort() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() throws SQLException {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Byte.class, value);

        return getValue().byteValue();
    }

}