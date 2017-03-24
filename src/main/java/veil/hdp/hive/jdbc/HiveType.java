package veil.hdp.hive.jdbc;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hive.service.cli.thrift.TTypeId;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;

public enum HiveType {

    VOID("VOID", TTypeId.NULL_TYPE, JDBCType.NULL, null),
    BOOLEAN("BOOLEAN", TTypeId.BOOLEAN_TYPE, JDBCType.BOOLEAN, Boolean.class),
    TINY_INT("TINYINT", TTypeId.TINYINT_TYPE, JDBCType.TINYINT, Byte.class),
    SMALL_INT("SMALLINT", TTypeId.SMALLINT_TYPE, JDBCType.SMALLINT, Short.class),
    INTEGER("INT", TTypeId.INT_TYPE, JDBCType.INTEGER, Integer.class),
    BIG_INT("BIGINT", TTypeId.BIGINT_TYPE, JDBCType.BIGINT, Long.class),
    FLOAT("FLOAT", TTypeId.FLOAT_TYPE, JDBCType.FLOAT, Float.class),
    DOUBLE("DOUBLE", TTypeId.DOUBLE_TYPE, JDBCType.DOUBLE, Double.class),
    STRING("STRING", TTypeId.STRING_TYPE, JDBCType.VARCHAR, String.class),
    CHAR("CHAR", TTypeId.CHAR_TYPE, JDBCType.CHAR, Character.class),
    VARCHAR("VARCHAR", TTypeId.VARCHAR_TYPE, JDBCType.VARCHAR, String.class),
    DATE("DATE", TTypeId.DATE_TYPE, JDBCType.DATE, Date.class),
    TIMESTAMP("TIMESTAMP", TTypeId.TIMESTAMP_TYPE, JDBCType.TIMESTAMP, Timestamp.class),
    //TIME("TIME", null, JDBCType.TIME, Time.class),
    DECIMAL("DECIMAL", TTypeId.DECIMAL_TYPE, JDBCType.DECIMAL, BigDecimal.class),
    BINARY("BINARY", TTypeId.BINARY_TYPE, JDBCType.BINARY, byte[].class),

    // todo: don't understand USER_DEFINED
    USER_DEFINED("USER_DEFINED", TTypeId.USER_DEFINED_TYPE, JDBCType.JAVA_OBJECT, Object.class),

    // todo: don't understand UNION
    UNION("UNION", TTypeId.UNION_TYPE, JDBCType.JAVA_OBJECT, Object.class),

    // todo: don't understand MAP
    MAP("MAP", TTypeId.MAP_TYPE, JDBCType.JAVA_OBJECT, Map.class),

    // todo: don't understand INTERVAL_YEAR_MONTH
    INTERVAL_YEAR_MONTH("INTERVAL_YEAR_MONTH", TTypeId.INTERVAL_YEAR_MONTH_TYPE, JDBCType.JAVA_OBJECT, HiveIntervalYearMonth.class),

    // todo: don't understand INTERVAL_DAY_TIME
    INTERVAL_DAY_TIME("INTERVAL_DAY_TIME", TTypeId.INTERVAL_DAY_TIME_TYPE, JDBCType.JAVA_OBJECT, HiveIntervalDayTime.class),

    // todo: don't understand ARRAY
    ARRAY("ARRAY", TTypeId.ARRAY_TYPE, JDBCType.ARRAY, Array.class),

    // todo: don't understand STRUCT
    STRUCT("STRUCT", TTypeId.STRUCT_TYPE, JDBCType.STRUCT, Struct.class);

    private final String name;
    private final TTypeId thriftType;
    private final JDBCType jdbcType;
    private final Class javaType;

    HiveType(String name, TTypeId thriftType, JDBCType jdbcType, Class javaType) {
        this.name = name;
        this.thriftType = thriftType;
        this.jdbcType = jdbcType;
        this.javaType = javaType;
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

    public static HiveType valueOf(TTypeId tTypeId) {
        for (HiveType hiveType : values()) {
            TTypeId thriftType = hiveType.getThriftType();
            if (thriftType != null && thriftType.equals(tTypeId)) {
                return hiveType;
            }
        }

        throw new IllegalArgumentException("Unrecognized Thrift TTypeId value: " + tTypeId);
    }
}
