package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.utils.SqlDateTimeUtils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public class TimestampColumn extends BaseColumn<Timestamp> {

        /*
    resist temptation to convert timestamp/date to long.  java.sql.date does not need/want time and doesn't fit spec
    furthermore, time as long assumes timezone as GMT and can cause unexpected results.
    for hive always use string representation and static helpers on java.sql.Date to construct
     */

    TimestampColumn(Timestamp value) {
        super(value);
    }

    @Override
    public Timestamp asTimestamp() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            // should be YYYY-MM-DD HH:MM:SS.fffffffff
            return value.toString();
        }

        return null;
    }

    @Override
    public Date asDate() throws SQLException {
        log.warn("will lose data going from {} to {}; value [{}]", Timestamp.class, Date.class, value);

        return SqlDateTimeUtils.convertTimestampToDate(value);
    }

    @Override
    public Time asTime() throws SQLException {
        log.warn("will lose data going from {} to {}; value [{}]", Timestamp.class, Time.class, value);

        return SqlDateTimeUtils.convertTimestampToTime(value);
    }

}
