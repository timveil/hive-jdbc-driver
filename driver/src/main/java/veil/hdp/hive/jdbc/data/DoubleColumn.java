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

public class DoubleColumn extends AbstractColumn<Double> {
    DoubleColumn(Double value) {
        super(value);
    }

    @Override
    public Double getValue() {
        return value != null ? value : 0;
    }

    @Override
    public Double asDouble() {
        return getValue();
    }

    @Override
    public String asString() {
        return Double.toString(getValue());
    }


    @Override
    public Integer asInt() {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Integer.class, value);

        return getValue().intValue();

    }

    @Override
    public Long asLong() {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Long.class, value);

        return getValue().longValue();
    }


    @Override
    public Float asFloat() {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Short.class, value);

        return getValue().floatValue();
    }

    @Override
    public Short asShort() {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Short.class, value);

        return getValue().shortValue();
    }


    @Override
    public Byte asByte() {
        log.warn("may lose precision going from {} to {}; value [{}]", Double.class, Byte.class, value);

        return getValue().byteValue();
    }

}
