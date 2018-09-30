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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static java.lang.Boolean.valueOf;

public class StringColumn extends AbstractColumn<String> {

    StringColumn(String value) {
        super(value);
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        if (value != null) {
            return valueOf(value);
        }

        return null;
    }

    @Override
    public Date asDate() {
        if (value != null) {
            return SqlDateTimeUtils.convertStringToDate(value);
        }

        return null;
    }

    @Override
    public Timestamp asTimestamp() {
        if (value != null) {
            return SqlDateTimeUtils.convertStringToTimestamp(value);
        }

        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (value != null) {
            return new BigDecimal(value);
        }

        return null;
    }

    @Override
    public Double asDouble() {
        if (value != null) {
            return Double.valueOf(value);
        }

        return null;
    }

    @Override
    public Float asFloat() {
        if (value != null) {
            return Float.valueOf(value);
        }

        return null;
    }

    @Override
    public Integer asInt() {
        if (value != null) {
            return Integer.valueOf(value);
        }

        return null;
    }

    @Override
    public Long asLong() {
        if (value != null) {
            return Long.valueOf(value);
        }

        return null;
    }

    @Override
    public Short asShort() {
        if (value != null) {
            return Short.valueOf(value);
        }

        return null;
    }

    @Override
    public Byte asByte() {
        if (value != null) {
            return Byte.valueOf(value);
        }

        return null;
    }

    @Override
    public Time asTime() {
        if (value != null) {
            return SqlDateTimeUtils.convertStringToTime(value);
        }

        return null;
    }

    @Override
    public InputStream asInputStream() {
        if (value != null) {
            return new ByteArrayInputStream(value.getBytes());
        }

        return null;
    }

    @Override
    public byte[] asByteArray() {
        if (value != null) {
            return value.getBytes();
        }

        return null;
    }

    @Override
    public Character asCharacter() {
        if (value != null) {

            if (value.length() != 1) {
                log.warn("may lose precision going from {} to {}; value [{}]", String.class, Character.class, value);
            }

            return value.charAt(0);
        }

        return null;
    }
}
