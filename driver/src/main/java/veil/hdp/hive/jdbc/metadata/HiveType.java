package veil.hdp.hive.jdbc.metadata;

//import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;


import veil.hdp.hive.jdbc.bindings.TTypeId;

import java.math.BigDecimal;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Map;

public enum HiveType {

    VOID("VOID", TTypeId.NULL_TYPE, JDBCType.NULL, null, false, 0, 0),
    BOOLEAN("BOOLEAN", TTypeId.BOOLEAN_TYPE, JDBCType.BOOLEAN, Boolean.class, false, 0, 0),
    TINY_INT("TINYINT", TTypeId.TINYINT_TYPE, JDBCType.TINYINT, Byte.class, false, 3, 0),
    SMALL_INT("SMALLINT", TTypeId.SMALLINT_TYPE, JDBCType.SMALLINT, Short.class, false, 5, 0),
    INTEGER("INT", TTypeId.INT_TYPE, JDBCType.INTEGER, Integer.class, false, 10, 0),
    BIG_INT("BIGINT", TTypeId.BIGINT_TYPE, JDBCType.BIGINT, Long.class, false, 19, 0),
    FLOAT("FLOAT", TTypeId.FLOAT_TYPE, JDBCType.FLOAT, Float.class, false, 0, 0),
    DOUBLE("DOUBLE", TTypeId.DOUBLE_TYPE, JDBCType.DOUBLE, Double.class, false, 0, 0),
    STRING("STRING", TTypeId.STRING_TYPE, JDBCType.VARCHAR, String.class, false, 0, 0),
    CHAR("CHAR", TTypeId.CHAR_TYPE, JDBCType.CHAR, Character.class, false, 0, 0),
    VARCHAR("VARCHAR", TTypeId.VARCHAR_TYPE, JDBCType.VARCHAR, String.class, false, 0, 0),
    DATE("DATE", TTypeId.DATE_TYPE, JDBCType.DATE, Date.class, false, 0, 0),
    TIMESTAMP("TIMESTAMP", TTypeId.TIMESTAMP_TYPE, JDBCType.TIMESTAMP, Timestamp.class, false, 0, 0),
    DECIMAL("DECIMAL", TTypeId.DECIMAL_TYPE, JDBCType.DECIMAL, BigDecimal.class, false, 0, 0),
    BINARY("BINARY", TTypeId.BINARY_TYPE, JDBCType.BINARY, byte[].class, false, 0, 0),

    // todo: don't understand UNION
    UNION("UNION", TTypeId.UNION_TYPE, JDBCType.JAVA_OBJECT, Object.class, true, 0, 0),

    // todo: don't understand MAP
    MAP("MAP", TTypeId.MAP_TYPE, JDBCType.JAVA_OBJECT, Map.class, true, 0, 0),

    // todo: don't understand ARRAY
    ARRAY("ARRAY", TTypeId.ARRAY_TYPE, JDBCType.ARRAY, Array.class, true, 0, 0),

    // todo: don't understand STRUCT
    STRUCT("STRUCT", TTypeId.STRUCT_TYPE, JDBCType.STRUCT, Struct.class, true, 0, 0);

    // INTERVAL_YEAR_MONTH is not a table column type but rather something found in predicates
    // https://issues.apache.org/jira/browse/HIVE-9792
    // https://issues.apache.org/jira/browse/HIVE-13557
    //INTERVAL_YEAR_MONTH("INTERVAL_YEAR_MONTH", TTypeId.INTERVAL_YEAR_MONTH_TYPE, JDBCType.JAVA_OBJECT, HiveIntervalYearMonth.class, true, 0, 0),

    // INTERVAL_DAY_TIME; same as INTERVAL_YEAR_MONTH; doesn't seem to exist in Hive 2.
    //INTERVAL_DAY_TIME("INTERVAL_DAY_TIME", TTypeId.INTERVAL_DAY_TIME_TYPE, JDBCType.JAVA_OBJECT, HiveIntervalDayTime.class, true, 0, 0),

    // USER_DEFINED is not a valid column type
    //USER_DEFINED("USER_DEFINED", TTypeId.USER_DEFINED_TYPE, JDBCType.JAVA_OBJECT, Object.class, true, 0, 0),


    private final String name;
    private final TTypeId thriftType;
    private final JDBCType jdbcType;
    private final Class javaType;
    private final boolean complex;
    private final int precision;
    private final int scale;

    HiveType(String name, TTypeId thriftType, JDBCType jdbcType, Class javaType, boolean complex, int precision, int scale) {
        this.name = name;
        this.thriftType = thriftType;
        this.jdbcType = jdbcType;
        this.javaType = javaType;
        this.complex = complex;
        this.precision = precision;
        this.scale = scale;
    }

    public static HiveType valueOf(TTypeId tTypeId) {
        for (HiveType hiveType : values()) {
            TTypeId thriftType = hiveType.thriftType;
            if (thriftType != null && thriftType.equals(tTypeId)) {
                return hiveType;
            }
        }

        throw new IllegalArgumentException(MessageFormat.format("Unrecognized TTypeId [{0}]", tTypeId));
    }

    public String getName() {
        return name;
    }

    public TTypeId getThriftType() {
        return thriftType;
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public Class getJavaType() {
        return javaType;
    }

    public boolean isComplex() {
        return complex;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isSigned() {
        return this.equals(SMALL_INT) || this.equals(INTEGER) || this.equals(BIG_INT);
    }

    public boolean isCaseSensitive() {
        return this.equals(STRING);
    }

    public boolean isSearchable() {
        return !complex;
    }
}
