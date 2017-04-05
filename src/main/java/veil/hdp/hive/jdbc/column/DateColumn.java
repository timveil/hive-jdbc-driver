package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;
import veil.hdp.hive.jdbc.Constants;

import java.sql.Date;
import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
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
            return Constants.HIVE_DATE_FORMAT.format(value);
        }

        return null;
    }
}
