package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.Type;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.slf4j.LoggerFactory.getLogger;

public class ResultSetUtils {

    private static final Logger log = getLogger(ResultSetUtils.class);

    public static Object getColumnValue(TableSchema schema, Object[] row, int columnIndex, Type targetType) throws SQLException {

        try {
            validateRow(row, columnIndex);

            Object value = row[columnIndex - 1];

            ColumnDescriptor columnDescriptor = schema.getColumns().get(columnIndex - 1);

            Type columnType = columnDescriptor.getTypeDescriptor().getType();


            if (targetType != null && !columnType.equals(targetType)) {
                log.trace("target type [{}] does not match column type [{}] with value [{}] for column [{}].  you should consider using a different method on the ResultSet interface", targetType, columnType, value, columnDescriptor.getNormalizedName());
            }

            if (targetType == null) {
                log.info("target type is null. setting to column type [{}]", columnType);
                targetType = columnType;
            }

            return convert(value, columnType, targetType);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }

    }

    public static InputStream getColumnValue(TableSchema schema, Object[] row, int columnIndex) throws SQLException {

        try {
            validateRow(row, columnIndex);

            Object value = row[columnIndex - 1];

            ColumnDescriptor columnDescriptor = schema.getColumns().get(columnIndex - 1);

            Type columnType = columnDescriptor.getTypeDescriptor().getType();

            return convertToInputStream(value, columnType);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }

    }

    public static int findColumnIndex(TableSchema tableSchema, String columnLabel) throws SQLException {
        ColumnDescriptor columnDescriptorForName = tableSchema.getColumn(columnLabel);

        if (columnDescriptorForName != null) {
            return columnDescriptorForName.getPosition();
        }

        throw new SQLException("Could not find column for name " + columnLabel + " in TableSchema " + tableSchema);
    }

    private static void validateRow(Object[] row, int columnIndex) {
        if (row == null) {
            throw new IllegalArgumentException("row is null");
        }

        if (row.length == 0) {
            throw new IllegalArgumentException("row length is zero");
        }

        if (columnIndex > row.length) {
            throw new IllegalArgumentException("invalid columnIndex [" + columnIndex + "] for row length [" + row.length + "]");
        }
    }

    private static Object convert(Object value, Type columnType, Type targetType) throws UnsupportedEncodingException {

        switch (targetType) {

            case BOOLEAN_TYPE:
                return convertToBoolean(value, columnType);
            case TINYINT_TYPE:
                return convertToByte(value, columnType);
            case SMALLINT_TYPE:
                return convertToShort(value, columnType);
            case INT_TYPE:
                return convertToInt(value, columnType);
            case BIGINT_TYPE:
                return convertToLong(value, columnType);
            case FLOAT_TYPE:
                return convertToFloat(value, columnType);
            case DOUBLE_TYPE:
                return convertToDouble(value, columnType);
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return convertToString(value, columnType);
            case DATE_TYPE:
                return convertToDate(value, columnType);
            case TIMESTAMP_TYPE:
                return convertToTimestamp(value, columnType);
            case DECIMAL_TYPE:
                return convertToBigDecimal(value, columnType);
            case BINARY_TYPE:
                return convertToByteArray(value, columnType);
            case NULL_TYPE:
            case ARRAY_TYPE:
            case MAP_TYPE:
            case STRUCT_TYPE:
            case UNION_TYPE:
            case USER_DEFINED_TYPE:
            case INTERVAL_YEAR_MONTH_TYPE:
            case INTERVAL_DAY_TIME_TYPE:
                log.warn("no conversion strategy for target type [{}] and value [{}] value class is [{}].  method returns original value.", targetType, value, value != null ? value.getClass() : null);
        }

        return value;
    }


    private static byte[] convertToByteArray(Object value, Type columnType) {

        if (value == null) {
            return null;
        }

        String stringRepresentation = convertToString(value, columnType);

        return stringRepresentation.getBytes(StandardCharsets.UTF_8);
    }

    private static InputStream convertToInputStream(Object value, Type columnType) {

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

    private static BigDecimal convertToBigDecimal(Object value, Type columnType) {

        if (value == null) {
            return null;
        }

        switch (columnType) {
            case DECIMAL_TYPE:
                return (BigDecimal) value;
            case DOUBLE_TYPE:
                return BigDecimal.valueOf((Double) value);
            case BIGINT_TYPE:
                return BigDecimal.valueOf((Long) value);
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return new BigDecimal((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to BigDecimal from column type [" + columnType + "]");
    }

    private static byte convertToByte(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINYINT_TYPE:
                return ((Number) value).byteValue();
            case SMALLINT_TYPE:
                log.warn("may lose precision going from short to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case INT_TYPE:
                log.warn("may lose precision going from int to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case BIGINT_TYPE:
                log.warn("may lose precision going from long to byte; value [{}]", value.toString());
                return ((Number) value).byteValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Byte.parseByte((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Byte from column type [" + columnType + "]");
    }

    private static short convertToShort(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
                return ((Number) value).shortValue();
            case INT_TYPE:
                log.warn("may lose precision going from int to short; value [{}]", value.toString());
                return ((Number) value).shortValue();
            case BIGINT_TYPE:
                log.warn("may lose precision going from long to short; value [{}]", value.toString());
                return ((Number) value).shortValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Short.parseShort((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Short from column type [" + columnType + "]");
    }

    private static int convertToInt(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
            case INT_TYPE:
                return ((Number) value).intValue();
            case BIGINT_TYPE:
                log.warn("may lose precision going from long to int; value [{}]", value.toString());
                return ((Number) value).intValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Integer.parseInt((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Integer from column type [" + columnType + "]");
    }

    private static long convertToLong(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
            case INT_TYPE:
            case BIGINT_TYPE:
                return ((Number) value).longValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Long.parseLong((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Long from column type [" + columnType + "]");
    }

    private static float convertToFloat(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case FLOAT_TYPE:
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
            case INT_TYPE:
            case BIGINT_TYPE:
                return ((Number) value).floatValue();
            case DOUBLE_TYPE:
                log.warn("may lose precision going from double to float; value [{}]", value.toString());
                return ((Number) value).floatValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Float.parseFloat((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Float from column type [" + columnType + "]");
    }

    private static double convertToDouble(Object value, Type columnType) {

        if (value == null) {
            return 0;
        }

        switch (columnType) {
            case DOUBLE_TYPE:
            case FLOAT_TYPE:
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
            case INT_TYPE:
            case BIGINT_TYPE:
                return ((Number) value).doubleValue();
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Double.parseDouble((String) value);
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Double from column type [" + columnType + "]");
    }

    private static String convertToString(Object value, Type columnType) {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case BOOLEAN_TYPE:
                return Boolean.toString((Boolean) value);
            case TINYINT_TYPE:
                return Byte.toString((Byte) value);
            case SMALLINT_TYPE:
                return Short.toString((Short) value);
            case INT_TYPE:
                return Integer.toString((Integer) value);
            case BIGINT_TYPE:
                return Long.toString((Long) value);
            case FLOAT_TYPE:
                return Float.toString((Float) value);
            case DOUBLE_TYPE:
                return Double.toString((Double) value);
            case VARCHAR_TYPE:
                return value.toString();
            case STRING_TYPE:
                return value.toString();
            case CHAR_TYPE:
                return value.toString();
            case DATE_TYPE:
                return value.toString();
            case TIMESTAMP_TYPE:
                return value.toString();
            case DECIMAL_TYPE:
                return value.toString();
            case BINARY_TYPE:
                return new String((byte[]) value);
            case ARRAY_TYPE:
                return Arrays.toString((Object[]) value);
            case INTERVAL_YEAR_MONTH_TYPE:
                break;
            case INTERVAL_DAY_TIME_TYPE:
                break;
            case MAP_TYPE:
                break;
            case STRUCT_TYPE:
                break;
            case UNION_TYPE:
                break;
            case NULL_TYPE:
                break;
            case USER_DEFINED_TYPE:
                break;
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to String from column type [" + columnType + "]");

    }

    private static Date convertToDate(Object value, Type columnType) {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Date.valueOf((String) value);
            case DATE_TYPE:
                return (Date) value;
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Date from column type [" + columnType + "]");
    }

    private static Timestamp convertToTimestamp(Object value, Type columnType) {

        if (value == null) {
            return null;
        }

        switch (columnType) {

            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return Timestamp.valueOf((String) value);
            case TIMESTAMP_TYPE:
                return (Timestamp) value;
        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Timestamp from column type [" + columnType + "]");
    }

    private static Boolean convertToBoolean(Object value, Type columnType) {

        if (value == null) {
            return false;
        }

        switch (columnType) {

            case BOOLEAN_TYPE:
                return (Boolean) value;
            case TINYINT_TYPE:
                return ((Number) value).byteValue() == 1;
            case SMALLINT_TYPE:
                return ((Number) value).shortValue() == 1;
            case INT_TYPE:
                return ((Number) value).intValue() == 1;
            case BIGINT_TYPE:
                return ((Number) value).longValue() == 1;
            case STRING_TYPE:
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return value.equals("1")
                        || ((String) value).equalsIgnoreCase("yes")
                        || ((String) value).equalsIgnoreCase("true");

        }

        throw new IllegalArgumentException("no strategy to convert [" + value.toString() + "] to Boolean from column type [" + columnType + "]");
    }
}
