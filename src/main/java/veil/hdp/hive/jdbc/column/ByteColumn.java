package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
public class ByteColumn extends AbstractColumn<Byte> {
    public ByteColumn(ColumnDescriptor descriptor, Byte value) {
        super(descriptor, value);
    }

    @Override
    public Byte getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Byte asByte() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return Byte.toString(value);
        }

        return null;
    }

    @Override
    public Boolean asBoolean() throws SQLException {
        if (value != null) {
            return value == 1;
        }

        return null;
    }
}
