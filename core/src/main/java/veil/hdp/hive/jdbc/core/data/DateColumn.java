package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.sql.Date;
import java.sql.SQLException;

public class DateColumn extends BaseColumn<Date> {

    /*
    resist temptation to convert timestamp/date to long.  java.sql.date does not need/want time and doesn't fit spec
    furthermore, time as long assumes timezone as GMT and can cause unexpected results.
    for hive always use string representation and static helpers on java.sql.Date to construct
     */

    DateColumn(ColumnDescriptor descriptor, Date value) {
        super(descriptor, value);
    }

    @Override
    public Date asDate() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            // should be YYYY-MM-DD
            return value.toString();
        }

        return null;
    }
}
