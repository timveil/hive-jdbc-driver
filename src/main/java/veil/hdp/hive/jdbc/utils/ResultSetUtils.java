package veil.hdp.hive.jdbc.utils;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.Type;
import org.slf4j.Logger;
import veil.hdp.hive.jdbc.metadata.TableSchema;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.slf4j.LoggerFactory.getLogger;

public class ResultSetUtils {

    private static final Logger log = getLogger(ResultSetUtils.class);

    public static Object getColumnValue(TableSchema schema, Object[] row, int columnIndex) throws SQLException {

        try {
            validateRow(row, columnIndex);

            Type columnType = schema.getColumnDescriptorAt(columnIndex - 1).getType();

            Object value = row[columnIndex - 1];

            return evaluate(columnType, value);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }

    }

    public static int findColumnIndex(TableSchema tableSchema, String columnLabel) throws SQLException {
        ColumnDescriptor columnDescriptorForName = tableSchema.getColumnDescriptorForName(columnLabel);

        if (columnDescriptorForName != null) {
            return columnDescriptorForName.getOrdinalPosition();
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

    private static Object evaluate(Type type, Object value) {

        switch (type) {
            case BINARY_TYPE:
                return evaluateBinary(value);
            case TIMESTAMP_TYPE:
                return evaluateTimestamp(value);
            case DECIMAL_TYPE:
                return evaluateBigDecimal(value);
            case BOOLEAN_TYPE:
                return evaluateBoolean(value);
            case SMALLINT_TYPE:
                return evaluateShort(value);
            case INT_TYPE:
                return evaluateInt(value);
            case BIGINT_TYPE:
                return evaluateLong(value);
            case FLOAT_TYPE:
                return evaluateFloat(value);
            case DOUBLE_TYPE:
                return evaluateDouble(value);
            case STRING_TYPE:
                return evaluateString(value);
            case DATE_TYPE:
                return evaluateDate(value);
            case INTERVAL_YEAR_MONTH_TYPE:
                return HiveIntervalYearMonth.valueOf((String) value);
            case INTERVAL_DAY_TIME_TYPE:
                return HiveIntervalDayTime.valueOf((String) value);
            default:

                if (log.isDebugEnabled()) {
                    log.debug("no special handling for type {}", type.getName());
                }

                return value;
        }
    }

    private static byte[] evaluateBinary(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return ((String) obj).getBytes();
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to byte[]");
    }

    private static Timestamp evaluateTimestamp(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Timestamp) {
            return (Timestamp) obj;
        } else if (obj instanceof String) {
            return Timestamp.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to Timestamp");
    }

    private static String evaluateString(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof byte[]) {
            return new String((byte[]) obj);
        } else if (String.class.isInstance(obj)) {
            return (String) obj;
        }

        return obj.toString();
    }

    private static short evaluateShort(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (Number.class.isInstance(obj)) {
            return ((Number) obj).shortValue();
        } else if (String.class.isInstance(obj)) {
            return Short.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to short");
    }

    private static long evaluateLong(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (Number.class.isInstance(obj)) {
            return ((Number) obj).longValue();
        } else if (String.class.isInstance(obj)) {
            return Long.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to long");
    }

    private static int evaluateInt(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (Number.class.isInstance(obj)) {
            return ((Number) obj).intValue();
        } else if (String.class.isInstance(obj)) {
            return Integer.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to int");

    }

    private static float evaluateFloat(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (Number.class.isInstance(obj)) {
            return ((Number) obj).floatValue();
        } else if (String.class.isInstance(obj)) {
            return Float.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to float");

    }

    private static double evaluateDouble(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (Number.class.isInstance(obj)) {
            return ((Number) obj).doubleValue();
        } else if (String.class.isInstance(obj)) {
            return Double.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to double");

    }

    private static Date evaluateDate(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Date) {
            return (Date) obj;
        } else if (obj instanceof String) {
            return Date.valueOf((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to Date");
    }

    private static BigDecimal evaluateBigDecimal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else if (obj instanceof String) {
            return new BigDecimal((String) obj);
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to BigDecimal");
    }

    private static Boolean evaluateBoolean(Object obj) {
        if (obj == null) {
            return false;
        }

        if (Boolean.class.isInstance(obj)) {
            return (Boolean) obj;
        } else if (Number.class.isInstance(obj)) {
            return ((Number) obj).intValue() != 0;
        } else if (String.class.isInstance(obj)) {
            return !obj.equals("0");
        }

        throw new IllegalArgumentException("unable to convert [" + obj.toString() + "] to Boolean");
    }
}
