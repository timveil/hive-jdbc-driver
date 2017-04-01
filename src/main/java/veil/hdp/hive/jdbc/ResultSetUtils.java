package veil.hdp.hive.jdbc;

import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

import static org.slf4j.LoggerFactory.getLogger;

public class ResultSetUtils {

    private static final Logger log = getLogger(ResultSetUtils.class);

    public static Object getColumnValue(Schema schema, Object[] row, int columnIndex, HiveType targetType) throws SQLException {

        validateRow(row, columnIndex);

        Object value = row[columnIndex - 1];

        Column column = schema.getColumns().get(columnIndex - 1);

        HiveType columnType = column.getColumnType().getHiveType();

        if (targetType != null && !columnType.equals(targetType)) {
            if (log.isTraceEnabled()) {
                log.trace("target type [{}] does not match column type [{}] with value [{}] for column [{}].  you should consider using a different method on the ResultSet interface", targetType, columnType, value, column.getNormalizedName());
            }
        }

        if (targetType == null) {
            if (log.isTraceEnabled()) {
                log.trace("target type is null. setting to column type [{}]", columnType);
            }
            targetType = columnType;
        }

        return convert(value, columnType, targetType);


    }

    public static InputStream getColumnValueAsStream(Schema schema, Object[] row, int columnIndex) throws SQLException {

        validateRow(row, columnIndex);

        Object value = row[columnIndex - 1];

        Column column = schema.getColumns().get(columnIndex - 1);

        HiveType columnType = column.getColumnType().getHiveType();

        return convertToInputStream(value, columnType);
    }

    public static Time getColumnValueAsTime(Schema schema, Object[] row, int columnIndex) throws SQLException {

        validateRow(row, columnIndex);

        Object value = row[columnIndex - 1];

        Column column = schema.getColumns().get(columnIndex - 1);

        HiveType columnType = column.getColumnType().getHiveType();

        return convertToTime(value, columnType);
    }

    public static int findColumnIndex(Schema schema, String columnLabel) throws SQLException {
        Column column = schema.getColumn(columnLabel);

        if (column != null) {
            return column.getPosition();
        }

        throw new SQLException("Could not find column for name " + columnLabel + " in Schema " + schema);
    }

    private static void validateRow(Object[] row, int columnIndex) throws SQLException {
        if (row == null) {
            throw new SQLException("row is null");
        }

        if (row.length == 0) {
            throw new SQLException("row length is zero");
        }

        if (columnIndex > row.length) {
            throw new SQLException("invalid columnIndex [" + columnIndex + "] for row length [" + row.length + "]");
        }
    }

    private static Object convert(Object value, HiveType columnType, HiveType targetType) throws SQLDataException {

        switch (targetType) {

            case BOOLEAN:
                return convertToBoolean(value, columnType);
            case TINY_INT:
                return convertToByte(value, columnType);
            case SMALL_INT:
                return convertToShort(value, columnType);
            case INTEGER:
                return convertToInt(value, columnType);
            case BIG_INT:
                return convertToLong(value, columnType);
            case FLOAT:
                return convertToFloat(value, columnType);
            case DOUBLE:
                return convertToDouble(value, columnType);
            case STRING:
            case CHAR:
            case VARCHAR:
                return convertToString(value, columnType);
            case DATE:
                return convertToDate(value, columnType);
            case TIMESTAMP:
                return convertToTimestamp(value, columnType);
            case DECIMAL:
                return convertToBigDecimal(value, columnType);
            case BINARY:
                return convertToByteArray(value, columnType);
            case VOID:
            case ARRAY:
            case MAP:
            case STRUCT:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
                log.warn("no conversion strategy for target type [{}] and value [{}] value class is [{}].  method returns original value.", targetType, value, value != null ? value.getClass() : null);
        }

        return value;
    }


    private static byte[] convertToByteArray(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        String stringRepresentation = convertToString(value, columnType);

        return stringRepresentation.getBytes(StandardCharsets.UTF_8);
    }

    private static InputStream convertToInputStream(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        // todo: need to test whats actually happening here
        if (value instanceof InputStream) {
            return (InputStream) value;
        } else if (value instanceof byte[]) {
            return new ByteArrayInputStream((byte[]) value);
        } else {
            return new ByteArrayInputStream(convertToString(value, columnType).getBytes(StandardCharsets.UTF_8));
        }

    }

    private static BigDecimal convertToBigDecimal(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        switch (columnType) {
            case DECIMAL:
                return (BigDecimal) value;
            case DOUBLE:
                return BigDecimal.valueOf((Double) value);
            case BIG_INT:
                return BigDecimal.valueOf((Long) value);
            case STRING:
            case CHAR:
            case VARCHAR:
                return new BigDecimal((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to BigDecimal from column type [" + columnType + "]");
    }

    private static byte convertToByte(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINY_INT:
                return ((Number) value).byteValue();
            case SMALL_INT:
                log.warn("may lose precision going from short to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case INTEGER:
                log.warn("may lose precision going from int to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case BIG_INT:
                log.warn("may lose precision going from long to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Byte.parseByte((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Byte from column type [" + columnType + "]");
    }

    private static short convertToShort(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINY_INT:
            case SMALL_INT:
                return ((Number) value).shortValue();
            case INTEGER:
                log.warn("may lose precision going from int to short; value [{}]", value.toString());
                return ((Number) value).shortValue();
            case BIG_INT:
                log.warn("may lose precision going from long to short; value [{}]", value.toString());
                return ((Number) value).shortValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Short.parseShort((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Short from column type [" + columnType + "]");
    }

    private static int convertToInt(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINY_INT:
            case SMALL_INT:
            case INTEGER:
                return ((Number) value).intValue();
            case BIG_INT:
                log.warn("may lose precision going from long to int; value [{}]", value.toString());
                return ((Number) value).intValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Integer.parseInt((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Integer from column type [" + columnType + "]");
    }

    private static long convertToLong(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINY_INT:
            case SMALL_INT:
            case INTEGER:
            case BIG_INT:
                return ((Number) value).longValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Long.parseLong((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Long from column type [" + columnType + "]");
    }

    private static float convertToFloat(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case FLOAT:
            case TINY_INT:
            case SMALL_INT:
            case INTEGER:
            case BIG_INT:
                return ((Number) value).floatValue();
            case DOUBLE:
                log.warn("may lose precision going from double to float; value [{}]", value.toString());
                return ((Number) value).floatValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Float.parseFloat((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Float from column type [" + columnType + "]");
    }

    private static double convertToDouble(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case DOUBLE:
            case FLOAT:
            case TINY_INT:
            case SMALL_INT:
            case INTEGER:
            case BIG_INT:
                return ((Number) value).doubleValue();
            case STRING:
            case CHAR:
            case VARCHAR:
                return Double.parseDouble((String) value);
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Double from column type [" + columnType + "]");
    }

    private static String convertToString(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case BOOLEAN:
                return Boolean.toString((Boolean) value);
            case TINY_INT:
                return Byte.toString((Byte) value);
            case SMALL_INT:
                return Short.toString((Short) value);
            case INTEGER:
                return Integer.toString((Integer) value);
            case BIG_INT:
                return Long.toString((Long) value);
            case FLOAT:
                return Float.toString((Float) value);
            case DOUBLE:
                return Double.toString((Double) value);
            case VARCHAR:
                return value.toString();
            case STRING:
                return value.toString();
            case CHAR:
                return value.toString();
            case DATE:
                return value.toString();
            case TIMESTAMP:
                return value.toString();
            case DECIMAL:
                return value.toString();
            case BINARY:
                return new String((byte[]) value);
            case ARRAY:
                return Arrays.toString((Object[]) value);
            case INTERVAL_DAY_TIME:
                break;
            case INTERVAL_YEAR_MONTH:
                break;
            case MAP:
                break;
            case STRUCT:
                break;
            case VOID:
                break;
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to String from column type [" + columnType + "]");

    }

    private static Date convertToDate(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case STRING:
            case CHAR:
            case VARCHAR:
                return Date.valueOf((String) value);
            case DATE:
                return (Date) value;
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Date from column type [" + columnType + "]");
    }

    private static Timestamp convertToTimestamp(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case STRING:
            case CHAR:
            case VARCHAR:
                return Timestamp.valueOf((String) value);
            case TIMESTAMP:
                return (Timestamp) value;
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Timestamp from column type [" + columnType + "]");
    }

    private static Time convertToTime(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case STRING:
            case CHAR:
            case VARCHAR:
                return Time.valueOf((String) value);
            case BIG_INT:
            case INTEGER:
                return new Time((Long) value);
            case TIMESTAMP:
                return new Time(((Timestamp) value).getTime());
        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Time from column type [" + columnType + "]");
    }

    private static Boolean convertToBoolean(Object value, HiveType columnType) throws SQLDataException {

        if (value == null) {
            return false;
        }

        switch (columnType) {

            case BOOLEAN:
                return (Boolean) value;
            case TINY_INT:
                return ((Number) value).byteValue() == 1;
            case SMALL_INT:
                return ((Number) value).shortValue() == 1;
            case INTEGER:
                return ((Number) value).intValue() == 1;
            case BIG_INT:
                return ((Number) value).longValue() == 1;
            case STRING:
            case CHAR:
            case VARCHAR:
                return value.equals("1")
                        || ((String) value).equalsIgnoreCase("yes")
                        || ((String) value).equalsIgnoreCase("true");

        }

        throw new SQLDataException("no strategy to convert [" + value.toString() + "] to Boolean from column type [" + columnType + "]");
    }
}
