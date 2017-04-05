package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Created by tveil on 4/4/17.
 */
public class TimestampColumn extends AbstractColumn<Timestamp> {
    public TimestampColumn(ColumnDescriptor descriptor, Timestamp value) {
        super(descriptor, value);
    }

    @Override
    public Timestamp asTimestamp() throws SQLException {
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
    public Date asDate() throws SQLException {
        if (value != null) {
            return new Date(value.getTime());
        }

        return null;
    }

    @Override
    public Long asLong() throws SQLException {
        if (value != null) {
            return value.getTime();
        }

        return null;
    }
}
