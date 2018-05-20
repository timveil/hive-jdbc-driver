package veil.hdp.hive.jdbc.data;

import java.sql.Date;
import java.sql.SQLException;

public class DateColumn extends AbstractColumn<Date> {

    /*
    resist temptation to convert timestamp/date to long.  java.sql.date does not need/want time and doesn't fit spec
    furthermore, time as long assumes timezone as GMT and can cause unexpected results.
    for hive always use string representation and static helpers on java.sql.Date to construct
     */

    DateColumn(Date value) {
        super(value);
    }

    @Override
    public Date asDate() {
        return value;
    }

    @Override
    public String asString() {
        if (value != null) {
            // should be YYYY-MM-DD
            return value.toString();
        }

        return null;
    }
}
