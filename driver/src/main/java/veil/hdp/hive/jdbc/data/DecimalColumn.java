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

import java.math.BigDecimal;

public class DecimalColumn extends AbstractColumn<BigDecimal> {
    DecimalColumn(BigDecimal value) {
        super(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public String asString() {
        if (value != null) {
            return value.toString();
        }
        return null;
    }


    @Override
    public Integer asInt() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Integer.class, value);

        if (value != null) {
            return value.intValue();
        }
        return null;
    }

    @Override
    public Long asLong() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Long.class, value);

        if (value != null) {
            return value.longValue();
        }
        return null;
    }

    @Override
    public Double asDouble() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Double.class, value);

        if (value != null) {
            return value.doubleValue();
        }
        return null;
    }

    @Override
    public Float asFloat() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Float.class, value);

        if (value != null) {
            return value.floatValue();
        }
        return null;
    }

    @Override
    public Short asShort() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Short.class, value);

        if (value != null) {
            return value.shortValue();
        }
        return null;
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", BigDecimal.class, Byte.class, value);

        if (value != null) {
            return value.byteValue();
        }
        return null;
    }
}
