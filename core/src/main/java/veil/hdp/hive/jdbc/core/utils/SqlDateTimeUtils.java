package veil.hdp.hive.jdbc.core.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class SqlDateTimeUtils {

    public static Date convertStringToDate(String dateString) {
        return Date.valueOf(dateString);
    }

    public static Timestamp convertStringToTimestamp(String timestampString) {
        return Timestamp.valueOf(timestampString);
    }

    public static Time convertStringToTime(String timeString) {
        return Time.valueOf(timeString);
    }

    public static Date convertTimestampToDate(Timestamp timestamp) {

        if (timestamp == null) {
            return null;
        }

        // assumes format of  YYYY-MM-DD HH:MM:SS.fffffffff

        String timestampString = timestamp.toString();

        String dateString = timestampString.substring(0, timestampString.indexOf(' '));

        return convertStringToDate(dateString);

    }

    public static Time convertTimestampToTime(Timestamp timestamp) {

        if (timestamp == null) {
            return null;
        }

        // assumes format of  YYYY-MM-DD HH:MM:SS.fffffffff

        String timestampString = timestamp.toString();

        int lastIndex = timestampString.length();

        if (timestampString.contains(".")) {
            lastIndex = timestampString.lastIndexOf('.');
        }

        String timeString = timestampString.substring(timestampString.indexOf(' ') + 1, lastIndex);

        return convertStringToTime(timeString);

    }

    /*public static void main(String[] args) {
        Timestamp ts = Timestamp.valueOf("2017-01-01 01:01:01");

        SqlDateTimeUtils.convertTimestampToTime(ts);


    }*/
}
