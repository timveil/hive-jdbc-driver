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

package veil.hdp.hive.jdbc;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HivePreparedStatement extends AbstractPreparedStatement {


    private static final char PLACEHOLDER = '?';
    private static final char SINGLE_QUOTE = '\'';
    private static final char DOUBLE_QUOTE = '"';
    private static final char BACKSLASH = '\\';
    private static final String NULL_STRING = "NULL";

    private static final char[] ESCAPE_CHARS = {SINGLE_QUOTE, BACKSLASH, DOUBLE_QUOTE};

    static {
        Arrays.sort(ESCAPE_CHARS);
    }

    private final String sql;

    private final Map<Integer, String> parameterValues;


    private HivePreparedStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, String sql) {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        this.sql = sql;
        parameterValues = new HashMap<>();
    }

    public static PreparedStatementBuilder builder() {
        return new PreparedStatementBuilder();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameterValues.clear();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(updateSql(sql));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(updateSql(sql));
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(updateSql(sql));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameterValues.put(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameterValues.put(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameterValues.put(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameterValues.put(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameterValues.put(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameterValues.put(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameterValues.put(parameterIndex, Boolean.toString(x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        String newValue = StringUtils.replace(x, Character.toString(SINGLE_QUOTE), String.valueOf(BACKSLASH) + SINGLE_QUOTE);
        parameterValues.put(parameterIndex, StringUtils.wrap(newValue, SINGLE_QUOTE));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameterValues.put(parameterIndex, StringUtils.wrap(x.toString(), SINGLE_QUOTE));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        parameterValues.put(parameterIndex, StringUtils.wrap(x.toString(), SINGLE_QUOTE));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        parameterValues.put(parameterIndex, StringUtils.wrap(x.toString(), SINGLE_QUOTE));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        parameterValues.put(parameterIndex, NULL_STRING);
    }


    // todo

    /*

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        super.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        super.setObject(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        super.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        super.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        super.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        super.setTimestamp(parameterIndex, x, cal);
    }


    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return super.getParameterMetaData();
    }



    */

    private String updateSql(String originalSql) {

        if (!originalSql.contains(String.valueOf(PLACEHOLDER))) {
            return originalSql;
        }

        StringBuilder builder = new StringBuilder(originalSql.length());

        int escapeCount = 0;
        int parameterCount = 1;

        for (int i = 0; i < originalSql.length(); i++) {
            char currentChar = sql.charAt(i);

            if (Arrays.binarySearch(ESCAPE_CHARS, currentChar) >= 0) {
                builder.append(currentChar);
                escapeCount++;
            } else if (currentChar == PLACEHOLDER && escapeCount % 2 == 0 && parameterValues.containsKey(parameterCount)) {
                builder.append(parameterValues.get(parameterCount));
                parameterCount++;
            } else {
                builder.append(currentChar);
            }
        }

        return builder.toString();

    }

    public static class PreparedStatementBuilder extends HiveStatementBuilder {

        private String sql;

        private PreparedStatementBuilder() {
        }

        PreparedStatementBuilder sql(String sql) {
            this.sql = sql;
            return this;
        }

        @Override
        PreparedStatementBuilder connection(HiveConnection connection) {
            super.connection = connection;
            return this;
        }

        @Override
        PreparedStatementBuilder type(int resultSetType) {
            super.resultSetType = resultSetType;
            return this;
        }

        @Override
        PreparedStatementBuilder concurrency(int resultSetConcurrency) {
            super.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        @Override
        PreparedStatementBuilder holdability(int resultSetHoldability) {
            super.resultSetHoldability = resultSetHoldability;
            return this;
        }

        public HivePreparedStatement build() {
            return new HivePreparedStatement(connection, resultSetType, resultSetConcurrency, resultSetHoldability, StringUtils.trim(sql));
        }
    }
}
