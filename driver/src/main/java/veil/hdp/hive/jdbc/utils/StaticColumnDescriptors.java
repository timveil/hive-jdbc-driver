package veil.hdp.hive.jdbc.utils;


import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.ColumnTypeDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;

import java.util.ArrayList;
import java.util.List;

final class StaticColumnDescriptors {


    private static final ColumnTypeDescriptor STRING = ColumnTypeDescriptor.builder().hiveType(HiveType.STRING).build();
    private static final ColumnTypeDescriptor INTEGER = ColumnTypeDescriptor.builder().hiveType(HiveType.INTEGER).build();
    private static final ColumnTypeDescriptor SMALL_INT = ColumnTypeDescriptor.builder().hiveType(HiveType.SMALL_INT).build();
    private static final ColumnTypeDescriptor BIG_INT = ColumnTypeDescriptor.builder().hiveType(HiveType.BIG_INT).build();
    private static final ColumnTypeDescriptor BOOLEAN = ColumnTypeDescriptor.builder().hiveType(HiveType.BOOLEAN).build();

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
        PK_NAME String => primary key name (may be null)
     */
    static final List<ColumnDescriptor> PRIMARY_KEYS = new ArrayList<>(6);


    static {

        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("PK_NAME").typeDescriptor(STRING).position(6).build());
        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("KEY_SEQ").typeDescriptor(INTEGER).position(5).build());
        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(4).build());
        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("TABLE_NAME").typeDescriptor(STRING).position(3).build());
        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("TABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        PRIMARY_KEYS.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        PROCEDURE_CAT String => procedure catalog (may be null)
        PROCEDURE_SCHEM String => procedure schema (may be null)
        PROCEDURE_NAME String => procedure name
        reserved for future use
        reserved for future use
        reserved for future use
        REMARKS String => explanatory comment on the procedure
        PROCEDURE_TYPE short => kind of procedure:
        SPECIFIC_NAME String => The name which uniquely identifies this procedure within its schema.
     */
    static final List<ColumnDescriptor> PROCEDURES = new ArrayList<>(9);

    static {
        PROCEDURES.add(ColumnDescriptor.builder().name("SPECIFIC_NAME").typeDescriptor(STRING).position(9).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("PROCEDURE_TYPE").typeDescriptor(SMALL_INT).position(8).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(7).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("RESERVED").typeDescriptor(STRING).position(6).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("RESERVED").typeDescriptor(STRING).position(5).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("RESERVED").typeDescriptor(STRING).position(4).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("PROCEDURE_NAME").typeDescriptor(STRING).position(3).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("PROCEDURE_SCHEM").typeDescriptor(STRING).position(2).build());
        PROCEDURES.add(ColumnDescriptor.builder().name("PROCEDURE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        PROCEDURE_CAT String => procedure catalog (may be null)
        PROCEDURE_SCHEM String => procedure schema (may be null)
        PROCEDURE_NAME String => procedure name
        COLUMN_NAME String => column/parameter name
        COLUMN_TYPE Short => kind of column/parameter:
        DATA_TYPE int => SQL type from java.sql.Types
        TYPE_NAME String => SQL type name, for a UDT type the type name is fully qualified
        PRECISION int => precision
        LENGTH int => length in bytes of data
        SCALE short => scale - null is returned for data types where SCALE is not applicable.
        RADIX short => radix
        NULLABLE short => can it contain NULL.
        REMARKS String => comment describing parameter/column
        COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
        SQL_DATA_TYPE int => reserved for future use
        SQL_DATETIME_SUB int => reserved for future use
        CHAR_OCTET_LENGTH int => the maximum length of binary and character based columns. For any other datatype the returned value is a NULL
        ORDINAL_POSITION int => the ordinal position, starting from 1, for the input and output parameters for a procedure. A value of 0 is returned if this row describes the procedure's return value. For result set columns, it is the ordinal position of the column in the result set starting from 1. If there are multiple result sets, the column ordinal positions are implementation defined.
        IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
        SPECIFIC_NAME String => the name which uniquely identifies this procedure within its schema.
     */
    static final List<ColumnDescriptor> PROCEDURE_COLUMNS = new ArrayList<>(20);

    static {
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("SPECIFIC_NAME").typeDescriptor(STRING).position(20).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("IS_NULLABLE").typeDescriptor(STRING).position(19).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("ORDINAL_POSITION").typeDescriptor(INTEGER).position(18).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("CHAR_OCTET_LENGTH").typeDescriptor(INTEGER).position(17).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("SQL_DATETIME_SUB").typeDescriptor(INTEGER).position(16).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("SQL_DATA_TYPE").typeDescriptor(INTEGER).position(15).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_DEF").typeDescriptor(STRING).position(14).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(13).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("NULLABLE").typeDescriptor(SMALL_INT).position(12).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("RADIX").typeDescriptor(SMALL_INT).position(11).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("SCALE").typeDescriptor(SMALL_INT).position(10).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("LENGTH").typeDescriptor(INTEGER).position(9).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("PRECISION").typeDescriptor(INTEGER).position(8).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(7).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("DATA_TYPE").typeDescriptor(INTEGER).position(6).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_TYPE").typeDescriptor(SMALL_INT).position(5).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(4).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("PROCEDURE_NAME").typeDescriptor(STRING).position(3).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("PROCEDURE_SCHEM").typeDescriptor(STRING).position(2).build());
        PROCEDURE_COLUMNS.add(ColumnDescriptor.builder().name("PROCEDURE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        GRANTOR String => grantor of access (may be null)
        GRANTEE String => grantee of access
        PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
        IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
     */
    static final List<ColumnDescriptor> COLUMN_PRIVILEGES = new ArrayList<>(8);

    static {
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("IS_GRANTABLE").typeDescriptor(STRING).position(8).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("PRIVILEGE").typeDescriptor(STRING).position(7).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("GRANTEE").typeDescriptor(STRING).position(6).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("GRANTOR").typeDescriptor(STRING).position(5).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(4).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_NAME").typeDescriptor(STRING).position(3).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        COLUMN_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        SCOPE short => actual scope of result
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL data type from java.sql.Types
        TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        COLUMN_SIZE int => precision
        BUFFER_LENGTH int => not used
        DECIMAL_DIGITS short => scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.
        PSEUDO_COLUMN short => is this a pseudo column like an Oracle ROWID
     */
    static final List<ColumnDescriptor> BEST_ROW_IDENTIFIER = new ArrayList<>(8);

    static {
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("PSEUDO_COLUMN").typeDescriptor(SMALL_INT).position(8).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("DECIMAL_DIGITS").typeDescriptor(SMALL_INT).position(7).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("BUFFER_LENGTH").typeDescriptor(INTEGER).position(6).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("COLUMN_SIZE").typeDescriptor(INTEGER).position(5).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(4).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("DATA_TYPE").typeDescriptor(INTEGER).position(3).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(2).build());
        BEST_ROW_IDENTIFIER.add(ColumnDescriptor.builder().name("SCOPE").typeDescriptor(SMALL_INT).position(1).build());
    }

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        GRANTOR String => grantor of access (may be null)
        GRANTEE String => grantee of access
        PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
        IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
     */
    static final List<ColumnDescriptor> TABLE_PRIVILEGES = new ArrayList<>(7);

    static {
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("IS_GRANTABLE").typeDescriptor(STRING).position(7).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("PRIVILEGE").typeDescriptor(STRING).position(6).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("GRANTEE").typeDescriptor(STRING).position(5).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("GRANTOR").typeDescriptor(STRING).position(4).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_NAME").typeDescriptor(STRING).position(3).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        TABLE_PRIVILEGES.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        SCOPE short => is not used
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL data type from java.sql.Types
        TYPE_NAME String => Data source-dependent type name
        COLUMN_SIZE int => precision
        BUFFER_LENGTH int => length of column value in bytes
        DECIMAL_DIGITS short => scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.
        PSEUDO_COLUMN short => whether this is pseudo column like an Oracle ROWID
     */
    static final List<ColumnDescriptor> VERSION_COLUMNS = BEST_ROW_IDENTIFIER;

    /*
        PKTABLE_CAT String => primary key table catalog being imported (may be null)
        PKTABLE_SCHEM String => primary key table schema being imported (may be null)
        PKTABLE_NAME String => primary key table name being imported
        PKCOLUMN_NAME String => primary key column name being imported
        FKTABLE_CAT String => foreign key table catalog (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null)
        FKTABLE_NAME String => foreign key table name
        FKCOLUMN_NAME String => foreign key column name
        KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
        DELETE_RULE short => What happens to the foreign key when primary is deleted.
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => primary key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
     */
    static final List<ColumnDescriptor> IMPORTED_KEYS = new ArrayList<>(14);

    static {
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("DEFERRABILITY").typeDescriptor(SMALL_INT).position(14).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("PK_NAME").typeDescriptor(STRING).position(13).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("FK_NAME").typeDescriptor(STRING).position(12).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("DELETE_RULE").typeDescriptor(SMALL_INT).position(11).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("UPDATE_RULE").typeDescriptor(SMALL_INT).position(10).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("KEY_SEQ").typeDescriptor(SMALL_INT).position(9).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("FKCOLUMN_NAME").typeDescriptor(STRING).position(8).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("FKTABLE_NAME").typeDescriptor(STRING).position(7).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("FKTABLE_SCHEM").typeDescriptor(STRING).position(6).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("FKTABLE_CAT").typeDescriptor(STRING).position(5).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("PKCOLUMN_NAME").typeDescriptor(STRING).position(4).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("PKTABLE_NAME").typeDescriptor(STRING).position(3).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("PKTABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        IMPORTED_KEYS.add(ColumnDescriptor.builder().name("PKTABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        PKTABLE_CAT String => primary key table catalog (may be null)
        PKTABLE_SCHEM String => primary key table schema (may be null)
        PKTABLE_NAME String => primary key table name
        PKCOLUMN_NAME String => primary key column name
        FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
        FKTABLE_NAME String => foreign key table name being exported
        FKCOLUMN_NAME String => foreign key column name being exported
        KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to foreign key when primary is updated:
        DELETE_RULE short => What happens to the foreign key when primary is deleted.
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => primary key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
     */
    static final List<ColumnDescriptor> EXPORTED_KEYS = IMPORTED_KEYS;

    /*
        PKTABLE_CAT String => parent key table catalog (may be null)
        PKTABLE_SCHEM String => parent key table schema (may be null)
        PKTABLE_NAME String => parent key table name
        PKCOLUMN_NAME String => parent key column name
        FKTABLE_CAT String => foreign key table catalog (may be null) being exported (may be null)
        FKTABLE_SCHEM String => foreign key table schema (may be null) being exported (may be null)
        FKTABLE_NAME String => foreign key table name being exported
        FKCOLUMN_NAME String => foreign key column name being exported
        KEY_SEQ short => sequence number within foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
        UPDATE_RULE short => What happens to foreign key when parent key is updated:
        DELETE_RULE short => What happens to the foreign key when parent key is deleted.
        FK_NAME String => foreign key name (may be null)
        PK_NAME String => parent key name (may be null)
        DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
     */
    static final List<ColumnDescriptor> CROSS_REFERENCE = IMPORTED_KEYS;

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
        INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
        INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
        TYPE short => index type:
        ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
        COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
        ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
        CARDINALITY long => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
        PAGES long => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
        FILTER_CONDITION String => Filter condition, if any. (may be null)
     */
    static final List<ColumnDescriptor> INDEX_INFO = new ArrayList<>(13);

    static {
        INDEX_INFO.add(ColumnDescriptor.builder().name("FILTER_CONDITION").typeDescriptor(STRING).position(13).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("PAGES").typeDescriptor(BIG_INT).position(12).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("CARDINALITY").typeDescriptor(BIG_INT).position(11).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("ASC_OR_DESC").typeDescriptor(STRING).position(10).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(9).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("ORDINAL_POSITION").typeDescriptor(SMALL_INT).position(8).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("TYPE").typeDescriptor(SMALL_INT).position(7).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("INDEX_NAME").typeDescriptor(STRING).position(6).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("INDEX_QUALIFIER").typeDescriptor(STRING).position(5).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("NON_UNIQUE").typeDescriptor(BOOLEAN).position(4).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("TABLE_NAME").typeDescriptor(STRING).position(3).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("TABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        INDEX_INFO.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TYPE_CAT String => the type's catalog (may be null)
        TYPE_SCHEM String => type's schema (may be null)
        TYPE_NAME String => type name
        CLASS_NAME String => Java class name
        DATA_TYPE int => type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
        REMARKS String => explanatory comment on the type
        BASE_TYPE short => type code of the source type of a DISTINCT type or the type
     */
    static final List<ColumnDescriptor> UDT = new ArrayList<>(7);

    static {
        UDT.add(ColumnDescriptor.builder().name("BASE_TYPE").typeDescriptor(SMALL_INT).position(7).build());
        UDT.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(6).build());
        UDT.add(ColumnDescriptor.builder().name("DATA_TYPE").typeDescriptor(INTEGER).position(5).build());
        UDT.add(ColumnDescriptor.builder().name("CLASS_NAME").typeDescriptor(STRING).position(4).build());
        UDT.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(3).build());
        UDT.add(ColumnDescriptor.builder().name("TYPE_SCHEM").typeDescriptor(STRING).position(2).build());
        UDT.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TYPE_CAT String => the UDT's catalog (may be null)
        TYPE_SCHEM String => UDT's schema (may be null)
        TYPE_NAME String => type name of the UDT
        SUPERTYPE_CAT String => the direct super type's catalog (may be null)
        SUPERTYPE_SCHEM String => the direct super type's schema (may be null)
        SUPERTYPE_NAME String => the direct super type's name
     */
    static final List<ColumnDescriptor> SUPER_TYPES = new ArrayList<>(6);

    static {
        SUPER_TYPES.add(ColumnDescriptor.builder().name("SUPERTYPE_NAME").typeDescriptor(STRING).position(6).build());
        SUPER_TYPES.add(ColumnDescriptor.builder().name("SUPERTYPE_SCHEM").typeDescriptor(STRING).position(5).build());
        SUPER_TYPES.add(ColumnDescriptor.builder().name("SUPERTYPE_CAT").typeDescriptor(STRING).position(4).build());
        SUPER_TYPES.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(3).build());
        SUPER_TYPES.add(ColumnDescriptor.builder().name("TYPE_SCHEM").typeDescriptor(STRING).position(2).build());
        SUPER_TYPES.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TABLE_CAT String => the type's catalog (may be null)
        TABLE_SCHEM String => type's schema (may be null)
        TABLE_NAME String => type name
        SUPERTABLE_NAME String => the direct super type's name
     */
    static final List<ColumnDescriptor> SUPER_TABLES = new ArrayList<>(4);

    static {
        SUPER_TABLES.add(ColumnDescriptor.builder().name("SUPERTABLE_NAME").typeDescriptor(STRING).position(4).build());
        SUPER_TABLES.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(3).build());
        SUPER_TABLES.add(ColumnDescriptor.builder().name("TYPE_SCHEM").typeDescriptor(STRING).position(2).build());
        SUPER_TABLES.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TYPE_CAT String => type catalog (may be null)
        TYPE_SCHEM String => type schema (may be null)
        TYPE_NAME String => type name
        ATTR_NAME String => attribute name
        DATA_TYPE int => attribute type SQL type from java.sql.Types
        ATTR_TYPE_NAME String => Data source dependent type name. For a UDT, the type name is fully qualified. For a REF, the type name is fully qualified and represents the target type of the reference type.
        ATTR_SIZE int => column size. For char or date types this is the maximum number of characters; for numeric or decimal types this is precision.
        DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        NULLABLE int => whether NULL is allowed
        REMARKS String => comment describing column (may be null)
        ATTR_DEF String => default value (may be null)
        SQL_DATA_TYPE int => unused
        SQL_DATETIME_SUB int => unused
        CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        ORDINAL_POSITION int => index of the attribute in the UDT (starting at 1)
        IS_NULLABLE String => ISO rules are used to determine the nullability for a attribute.
        SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        SCOPE_TABLE String => table name that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type,SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
     */
    static final List<ColumnDescriptor> ATTRIBUTES = new ArrayList<>(21);

    static {
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SOURCE_DATA_TYPE").typeDescriptor(SMALL_INT).position(21).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SCOPE_TABLE").typeDescriptor(STRING).position(20).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SCOPE_SCHEMA").typeDescriptor(STRING).position(19).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SCOPE_CATALOG").typeDescriptor(STRING).position(18).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("IS_NULLABLE").typeDescriptor(STRING).position(17).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("ORDINAL_POSITION").typeDescriptor(INTEGER).position(16).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("CHAR_OCTET_LENGTH").typeDescriptor(INTEGER).position(15).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SQL_DATETIME_SUB").typeDescriptor(INTEGER).position(14).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("SQL_DATA_TYPE").typeDescriptor(INTEGER).position(13).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("ATTR_DEF").typeDescriptor(STRING).position(12).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(11).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("NULLABLE").typeDescriptor(INTEGER).position(10).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("NUM_PREC_RADIX").typeDescriptor(INTEGER).position(9).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("DECIMAL_DIGITS").typeDescriptor(INTEGER).position(8).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("ATTR_SIZE").typeDescriptor(INTEGER).position(7).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("ATTR_TYPE_NAME").typeDescriptor(STRING).position(6).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("DATA_TYPE").typeDescriptor(INTEGER).position(5).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("ATTR_NAME").typeDescriptor(STRING).position(4).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("TYPE_NAME").typeDescriptor(STRING).position(3).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("TYPE_SCHEM").typeDescriptor(STRING).position(2).build());
        ATTRIBUTES.add(ColumnDescriptor.builder().name("TYPE_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        NAME String=> The name of the client info property
        MAX_LEN int=> The maximum length of the value for the property
        DEFAULT_VALUE String=> The default value of the property
        DESCRIPTION String=> A description of the property. This will typically contain information as to where this property is stored in the database.
     */
    static final List<ColumnDescriptor> CLIENT_INFO_PROPERTIES = new ArrayList<>(4);

    static {
        CLIENT_INFO_PROPERTIES.add(ColumnDescriptor.builder().name("DESCRIPTION").typeDescriptor(STRING).position(4).build());
        CLIENT_INFO_PROPERTIES.add(ColumnDescriptor.builder().name("DEFAULT_VALUE").typeDescriptor(STRING).position(3).build());
        CLIENT_INFO_PROPERTIES.add(ColumnDescriptor.builder().name("MAX_LEN").typeDescriptor(INTEGER).position(2).build());
        CLIENT_INFO_PROPERTIES.add(ColumnDescriptor.builder().name("NAME").typeDescriptor(STRING).position(1).build());
    }

    /*
        FUNCTION_CAT String => function catalog (may be null)
        FUNCTION_SCHEM String => function schema (may be null)
        FUNCTION_NAME String => function name. This is the name used to invoke the function
        REMARKS String => explanatory comment on the function
        FUNCTION_TYPE short => kind of function:
        SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload functions
     */
    static final List<ColumnDescriptor> FUNCTION_COLUMNS = new ArrayList<>(6);

    static {
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("SPECIFIC_NAME").typeDescriptor(STRING).position(6).build());
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("FUNCTION_TYPE").typeDescriptor(SMALL_INT).position(5).build());
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(4).build());
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("FUNCTION_NAME").typeDescriptor(STRING).position(3).build());
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("FUNCTION_SCHEM").typeDescriptor(STRING).position(2).build());
        FUNCTION_COLUMNS.add(ColumnDescriptor.builder().name("FUNCTION_CAT").typeDescriptor(STRING).position(1).build());
    }

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        DATA_TYPE int => SQL type from java.sql.Types
        COLUMN_SIZE int => column size.
        DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        COLUMN_USAGE String => The allowed usage for the column. The value returned will correspond to the enum name returned by PseudoColumnUsage.name()
        REMARKS String => comment describing column (may be null)
        CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
     */
    static final List<ColumnDescriptor> PSEUDO_COLUMNS = new ArrayList<>(12);

    static {
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("IS_NULLABLE").typeDescriptor(STRING).position(12).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("CHAR_OCTET_LENGTH").typeDescriptor(INTEGER).position(11).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("REMARKS").typeDescriptor(STRING).position(10).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_USAGE").typeDescriptor(STRING).position(9).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("NUM_PREC_RADIX").typeDescriptor(INTEGER).position(8).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("DECIMAL_DIGITS").typeDescriptor(INTEGER).position(7).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_SIZE").typeDescriptor(INTEGER).position(6).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("DATA_TYPE").typeDescriptor(INTEGER).position(5).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("COLUMN_NAME").typeDescriptor(STRING).position(4).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("TABLE_NAME").typeDescriptor(STRING).position(3).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("TABLE_SCHEM").typeDescriptor(STRING).position(2).build());
        PSEUDO_COLUMNS.add(ColumnDescriptor.builder().name("TABLE_CAT").typeDescriptor(STRING).position(1).build());
    }

    static final List<ColumnDescriptor> GENERATED_KEYS = new ArrayList<>(1);

    static {
        GENERATED_KEYS.add(ColumnDescriptor.builder().name("GENERATED_KEY").typeDescriptor(STRING).position(1).build());
    }

    static final List<ColumnDescriptor> QUERY_LOG = new ArrayList<>(1);

    static {
        QUERY_LOG.add(ColumnDescriptor.builder().name("STRING_VAL").typeDescriptor(STRING).position(1).build());
    }

    private StaticColumnDescriptors() {
    }
}
