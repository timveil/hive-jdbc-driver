package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public class TimestampColumn extends BaseColumn<Timestamp> {
    TimestampColumn(ColumnDescriptor descriptor, Timestamp value) {
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
    public Time asTime() throws SQLException {
        if (value != null) {
            return new Time(value.getTime());
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
