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

import java.sql.SQLException;

public class ShortColumn extends AbstractColumn<Short> {

    ShortColumn(Short value) {
        super(value);
    }

    @Override
    public Short getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Short asShort() {
        return getValue();
    }

    @Override
    public String asString() {
        return Short.toString(getValue());
    }

    @Override
    public Boolean asBoolean() {
        if (value != null) {
            return value == 1;
        }

        return null;
    }

    @Override
    public Float asFloat() {
        return getValue().floatValue();
    }


    @Override
    public Integer asInt() {
        return getValue().intValue();

    }

    @Override
    public Double asDouble() {
        return getValue().doubleValue();
    }

    @Override
    public Long asLong() {
        return getValue().longValue();
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", Short.class, Byte.class, value);

        return getValue().byteValue();
    }
}
