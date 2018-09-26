/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc.utils;

import veil.hdp.hive.jdbc.HiveException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public final class SqlDateTimeUtils {

    private static final char SPACE = ' ';
    private static final char DOT = '.';

    private SqlDateTimeUtils() {
    }

    public static Date convertStringToDate(String dateString) {
        try {
            return Date.valueOf(dateString);
        } catch (Exception e) {
            throw new HiveException("unable to convert string [" + dateString + "] to java.sql.Date.  must be in the following format yyyy-mm-dd", e);
        }
    }

    public static Timestamp convertStringToTimestamp(String timestampString) {
        try {
            return Timestamp.valueOf(timestampString);
        } catch (Exception e) {
            throw new HiveException("unable to convert string [" + timestampString + "] to java.sql.Timestamp.  must be in the following format yyyy-mm-dd hh:mm:ss.[fff...]", e);
        }
    }

    public static Time convertStringToTime(String timeString) {
        try {
            return Time.valueOf(timeString);
        } catch (Exception e) {
            throw new HiveException("unable to convert string [" + timeString + "] to java.sql.Time.  must be in the following format hh:mm:ss", e);
        }
    }

    public static Date convertTimestampToDate(Timestamp timestamp) {

        if (timestamp == null) {
            return null;
        }

        // assumes format of  YYYY-MM-DD HH:MM:SS.fffffffff

        String timestampString = timestamp.toString();

        String dateString = timestampString.substring(0, timestampString.indexOf(SPACE));

        return convertStringToDate(dateString);

    }

    public static Time convertTimestampToTime(Timestamp timestamp) {

        if (timestamp == null) {
            return null;
        }

        // assumes format of  YYYY-MM-DD HH:MM:SS.fffffffff

        String timestampString = timestamp.toString();

        int lastIndex = timestampString.length();

        if (timestampString.indexOf(DOT) > -1) {
            lastIndex = timestampString.lastIndexOf(DOT);
        }

        String timeString = timestampString.substring(timestampString.indexOf(SPACE) + 1, lastIndex);

        return convertStringToTime(timeString);

    }

    /*public static void main(String[] args) {
        Timestamp ts = Timestamp.valueOf("2017-01-01 01:01:01");

        SqlDateTimeUtils.convertTimestampToTime(ts);


    }*/
}
