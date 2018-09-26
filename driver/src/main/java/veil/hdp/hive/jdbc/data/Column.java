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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public interface Column<T> {

    T getValue();

    Boolean asBoolean() throws SQLException;

    Date asDate() throws SQLException;

    Timestamp asTimestamp() throws SQLException;

    BigDecimal asBigDecimal() throws SQLException;

    Double asDouble() throws SQLException;

    Float asFloat() throws SQLException;

    Integer asInt() throws SQLException;

    Long asLong() throws SQLException;

    Short asShort() throws SQLException;

    String asString() throws SQLException;

    Byte asByte() throws SQLException;

    byte[] asByteArray() throws SQLException;

    InputStream asInputStream() throws SQLException;

    Time asTime() throws SQLException;

    Character asCharacter() throws SQLException;

}
