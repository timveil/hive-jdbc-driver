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

package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.utils.SqlDateTimeUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class TimestampColumn extends AbstractColumn<Timestamp> {

        /*
    resist temptation to convert timestamp/date to long.  java.sql.date does not need/want time and doesn't fit spec
    furthermore, time as long assumes timezone as GMT and can cause unexpected results.
    for hive always use string representation and static helpers on java.sql.Date to construct
     */

    TimestampColumn(Timestamp value) {
        super(value);
    }

    @Override
    public Timestamp asTimestamp() {
        return value;
    }

    @Override
    public String asString() {
        if (value != null) {
            // should be YYYY-MM-DD HH:MM:SS.fffffffff
            return value.toString();
        }

        return null;
    }

    @Override
    public Date asDate() {
        log.warn("will lose data going from {} to {}; value [{}]", Timestamp.class, Date.class, value);

        return SqlDateTimeUtils.convertTimestampToDate(value);
    }

    @Override
    public Time asTime() {
        log.warn("will lose data going from {} to {}; value [{}]", Timestamp.class, Time.class, value);

        return SqlDateTimeUtils.convertTimestampToTime(value);
    }

}
