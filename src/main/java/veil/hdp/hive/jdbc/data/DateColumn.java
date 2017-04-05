package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.Date;
import java.sql.SQLException;

import static veil.hdp.hive.jdbc.utils.Constants.HIVE_DATE_FORMAT;

public class DateColumn extends AbstractColumn<Date> {


    public DateColumn(ColumnDescriptor descriptor, Date value) {
        super(descriptor, value);
    }

    @Override
    public Date asDate() throws SQLException {
        return getValue();
    }

    @Override
    public Long asLong() throws SQLException {
        if (value != null) {
            return value.getTime();
        }

        return null;
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return HIVE_DATE_FORMAT.format(value);
        }

        return null;
    }
}
