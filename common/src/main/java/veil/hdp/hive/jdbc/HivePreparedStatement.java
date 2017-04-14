package veil.hdp.hive.jdbc;

import java.io.InputStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HivePreparedStatement extends AbstractPreparedStatement {


    private static final char PLACEHOLDER = '?';

    private final String sql;

    private final Map<Integer, String> parameters;
    private final List<Integer> parameterIndexes;


    private HivePreparedStatement(HiveConnection connection, String sql, List<Integer> parameterIndexes, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        this.sql = sql;
        this.parameterIndexes = parameterIndexes;
        parameters = new HashMap<>();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
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
        parameters.put(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, Double.toString(x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        // todo: look at original driver
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        // todo: look at original driver
        super.setDate(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        // todo: look at original driver
        super.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        // todo: look at original driver
        super.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        // todo: look at original driver
        super.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        // todo: look at original driver
        super.setNull(parameterIndex, sqlType, typeName);
    }

    private String updateSql(String originalSql) {
        if (!originalSql.contains(String.valueOf(PLACEHOLDER))) {
            return originalSql;
        }

        StringBuilder builder = new StringBuilder(originalSql);

        for (int index : parameterIndexes) {

            if (parameters.containsKey(index)) {
                int adjustedIndex = index - 1;
                builder.deleteCharAt(adjustedIndex);
                builder.insert(adjustedIndex, parameterIndexes.get(index));
            } else {
                log.warn("######################### this is weird");
            }
        }

        return builder.toString();

    }


    public static class Builder {

        private HiveConnection connection;
        private String sql;
        private int resultSetType;
        private int resultSetConcurrency;
        private int resultSetHoldability;


        public HivePreparedStatement.Builder connection(HiveConnection connection) {
            this.connection = connection;
            return this;
        }

        public HivePreparedStatement.Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public HivePreparedStatement.Builder type(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        public HivePreparedStatement.Builder concurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public HivePreparedStatement.Builder holdability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }


        private List<Integer> findPlaceHolders(final String sql) {


            List<Integer> indexes = new ArrayList<>();

            int signalCount = 0;

            for (int i = 0; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (c == '\'' || c == '\\') {
                    // record the count of char "'" and char "\"
                    signalCount++;
                } else if (c == PLACEHOLDER && signalCount % 2 == 0) {
                    // check if the ? is really the parameter

                    // indexes are 1 based not 0 based so add one to normalize
                    indexes.add(i+1);

                }
            }

            return indexes;
        }

        public HivePreparedStatement build() {
            return new HivePreparedStatement(connection, sql, findPlaceHolders(sql), resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }
}
