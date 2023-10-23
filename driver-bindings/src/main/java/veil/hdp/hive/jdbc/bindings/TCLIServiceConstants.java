/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package veil.hdp.hive.jdbc.bindings;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TCLIServiceConstants {

  public static final java.util.Set<TTypeId> PRIMITIVE_TYPES = java.util.EnumSet.noneOf(TTypeId.class);
  static {
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.BOOLEAN_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.TINYINT_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.SMALLINT_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.INT_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.BIGINT_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.FLOAT_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.DOUBLE_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.STRING_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.TIMESTAMP_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.BINARY_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.DECIMAL_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.NULL_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.DATE_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.VARCHAR_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.CHAR_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.INTERVAL_YEAR_MONTH_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.INTERVAL_DAY_TIME_TYPE);
    PRIMITIVE_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.TIMESTAMPLOCALTZ_TYPE);
  }

  public static final java.util.Set<TTypeId> COMPLEX_TYPES = java.util.EnumSet.noneOf(TTypeId.class);
  static {
    COMPLEX_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.ARRAY_TYPE);
    COMPLEX_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.MAP_TYPE);
    COMPLEX_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.STRUCT_TYPE);
    COMPLEX_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.UNION_TYPE);
    COMPLEX_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.USER_DEFINED_TYPE);
  }

  public static final java.util.Set<TTypeId> COLLECTION_TYPES = java.util.EnumSet.noneOf(TTypeId.class);
  static {
    COLLECTION_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.ARRAY_TYPE);
    COLLECTION_TYPES.add(veil.hdp.hive.jdbc.bindings.TTypeId.MAP_TYPE);
  }

  public static final java.util.Map<TTypeId,java.lang.String> TYPE_NAMES = new java.util.EnumMap<TTypeId,java.lang.String>(TTypeId.class);
  static {
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.ARRAY_TYPE, "ARRAY");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.BIGINT_TYPE, "BIGINT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.BINARY_TYPE, "BINARY");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.BOOLEAN_TYPE, "BOOLEAN");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.CHAR_TYPE, "CHAR");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.DATE_TYPE, "DATE");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.DECIMAL_TYPE, "DECIMAL");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.DOUBLE_TYPE, "DOUBLE");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.FLOAT_TYPE, "FLOAT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.INTERVAL_DAY_TIME_TYPE, "INTERVAL_DAY_TIME");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.INTERVAL_YEAR_MONTH_TYPE, "INTERVAL_YEAR_MONTH");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.INT_TYPE, "INT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.MAP_TYPE, "MAP");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.NULL_TYPE, "NULL");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.SMALLINT_TYPE, "SMALLINT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.STRING_TYPE, "STRING");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.STRUCT_TYPE, "STRUCT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.TIMESTAMPLOCALTZ_TYPE, "TIMESTAMP WITH LOCAL TIME ZONE");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.TIMESTAMP_TYPE, "TIMESTAMP");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.TINYINT_TYPE, "TINYINT");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.UNION_TYPE, "UNIONTYPE");
    TYPE_NAMES.put(veil.hdp.hive.jdbc.bindings.TTypeId.VARCHAR_TYPE, "VARCHAR");
  }

  public static final java.lang.String CHARACTER_MAXIMUM_LENGTH = "characterMaximumLength";

  public static final java.lang.String PRECISION = "precision";

  public static final java.lang.String SCALE = "scale";

}
