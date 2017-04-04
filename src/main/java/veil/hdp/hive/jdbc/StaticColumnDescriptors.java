package veil.hdp.hive.jdbc;


import java.util.ArrayList;
import java.util.List;

public class StaticColumnDescriptors {

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        COLUMN_NAME String => column name
        KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
        PK_NAME String => primary key name (may be null)
     */
    public static final List<ColumnDescriptor> PRIMARY_KEYS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("KEY_SEQ", new ColumnTypeDescriptor(HiveType.INTEGER), 5));
            add(new ColumnDescriptor("PK_NAME", new ColumnTypeDescriptor(HiveType.STRING), 6));
        }

    };

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
    public static final List<ColumnDescriptor> PROCEDURES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("PROCEDURE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("PROCEDURE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("PROCEDURE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("RESERVED", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("RESERVED", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("RESERVED", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 7));
            add(new ColumnDescriptor("PROCEDURE_TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 8));
            add(new ColumnDescriptor("SPECIFIC_NAME", new ColumnTypeDescriptor(HiveType.STRING), 9));
        }

    };

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
    public static final List<ColumnDescriptor> PROCEDURE_COLUMNS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("PROCEDURE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("PROCEDURE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("PROCEDURE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("COLUMN_TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 5));
            add(new ColumnDescriptor("DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 6));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 7));
            add(new ColumnDescriptor("PRECISION", new ColumnTypeDescriptor(HiveType.INTEGER), 8));
            add(new ColumnDescriptor("LENGTH", new ColumnTypeDescriptor(HiveType.INTEGER), 9));
            add(new ColumnDescriptor("SCALE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 10));
            add(new ColumnDescriptor("RADIX", new ColumnTypeDescriptor(HiveType.SMALL_INT), 11));
            add(new ColumnDescriptor("NULLABLE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 12));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 13));
            add(new ColumnDescriptor("COLUMN_DEF", new ColumnTypeDescriptor(HiveType.STRING), 14));
            add(new ColumnDescriptor("SQL_DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 15));
            add(new ColumnDescriptor("SQL_DATETIME_SUB", new ColumnTypeDescriptor(HiveType.INTEGER), 16));
            add(new ColumnDescriptor("CHAR_OCTET_LENGTH", new ColumnTypeDescriptor(HiveType.INTEGER), 17));
            add(new ColumnDescriptor("ORDINAL_POSITION", new ColumnTypeDescriptor(HiveType.INTEGER), 18));
            add(new ColumnDescriptor("IS_NULLABLE", new ColumnTypeDescriptor(HiveType.STRING), 19));
            add(new ColumnDescriptor("SPECIFIC_NAME", new ColumnTypeDescriptor(HiveType.STRING), 20));
        }

    };

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
    public static final List<ColumnDescriptor> COLUMN_PRIVILEGES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("GRANTOR", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("GRANTEE", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("PRIVILEGE", new ColumnTypeDescriptor(HiveType.STRING), 7));
            add(new ColumnDescriptor("IS_GRANTABLE", new ColumnTypeDescriptor(HiveType.STRING), 8));
        }

    };

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
    public static final List<ColumnDescriptor> BEST_ROW_IDENTIFIER = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("SCOPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 1));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 3));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("COLUMN_SIZE", new ColumnTypeDescriptor(HiveType.INTEGER), 5));
            add(new ColumnDescriptor("BUFFER_LENGTH", new ColumnTypeDescriptor(HiveType.INTEGER), 6));
            add(new ColumnDescriptor("DECIMAL_DIGITS", new ColumnTypeDescriptor(HiveType.SMALL_INT), 7));
            add(new ColumnDescriptor("PSEUDO_COLUMN", new ColumnTypeDescriptor(HiveType.SMALL_INT), 8));
        }

    };

    /*
        TABLE_CAT String => table catalog (may be null)
        TABLE_SCHEM String => table schema (may be null)
        TABLE_NAME String => table name
        GRANTOR String => grantor of access (may be null)
        GRANTEE String => grantee of access
        PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
        IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
     */
    public static final List<ColumnDescriptor> TABLE_PRIVILEGES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("GRANTOR", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("GRANTEE", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("PRIVILEGE", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("IS_GRANTABLE", new ColumnTypeDescriptor(HiveType.STRING), 7));
        }

    };

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
    public static final List<ColumnDescriptor> VERSION_COLUMNS = BEST_ROW_IDENTIFIER;

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
    public static final List<ColumnDescriptor> IMPORTED_KEYS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("PKTABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("PKTABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("PKTABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("PKCOLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("FKTABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("FKTABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("FKTABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 7));
            add(new ColumnDescriptor("FKCOLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 8));
            add(new ColumnDescriptor("KEY_SEQ", new ColumnTypeDescriptor(HiveType.SMALL_INT), 9));
            add(new ColumnDescriptor("UPDATE_RULE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 10));
            add(new ColumnDescriptor("DELETE_RULE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 11));
            add(new ColumnDescriptor("FK_NAME", new ColumnTypeDescriptor(HiveType.STRING), 12));
            add(new ColumnDescriptor("PK_NAME", new ColumnTypeDescriptor(HiveType.STRING), 13));
            add(new ColumnDescriptor("DEFERRABILITY", new ColumnTypeDescriptor(HiveType.SMALL_INT), 14));
        }

    };

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
    public static final List<ColumnDescriptor> EXPORTED_KEYS = IMPORTED_KEYS;

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
    public static final List<ColumnDescriptor> CROSS_REFERENCE = IMPORTED_KEYS;

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
    public static final List<ColumnDescriptor> INDEX_INFO = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("NON_UNIQUE", new ColumnTypeDescriptor(HiveType.BOOLEAN), 4));
            add(new ColumnDescriptor("INDEX_QUALIFIER", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("INDEX_NAME", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 7));
            add(new ColumnDescriptor("ORDINAL_POSITION", new ColumnTypeDescriptor(HiveType.SMALL_INT), 8));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 9));
            add(new ColumnDescriptor("ASC_OR_DESC", new ColumnTypeDescriptor(HiveType.STRING), 10));
            add(new ColumnDescriptor("CARDINALITY", new ColumnTypeDescriptor(HiveType.BIG_INT), 11));
            add(new ColumnDescriptor("PAGES", new ColumnTypeDescriptor(HiveType.BIG_INT), 12));
            add(new ColumnDescriptor("FILTER_CONDITION", new ColumnTypeDescriptor(HiveType.STRING), 13));
        }

    };

    /*
        TYPE_CAT String => the type's catalog (may be null)
        TYPE_SCHEM String => type's schema (may be null)
        TYPE_NAME String => type name
        CLASS_NAME String => Java class name
        DATA_TYPE int => type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
        REMARKS String => explanatory comment on the type
        BASE_TYPE short => type code of the source type of a DISTINCT type or the type
     */
    public static final List<ColumnDescriptor> UDT = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TYPE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("CLASS_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 5));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("BASE_TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 7));
        }
    };

    /*
        TYPE_CAT String => the UDT's catalog (may be null)
        TYPE_SCHEM String => UDT's schema (may be null)
        TYPE_NAME String => type name of the UDT
        SUPERTYPE_CAT String => the direct super type's catalog (may be null)
        SUPERTYPE_SCHEM String => the direct super type's schema (may be null)
        SUPERTYPE_NAME String => the direct super type's name
     */
    public static final List<ColumnDescriptor> SUPER_TYPES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TYPE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("SUPERTYPE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("SUPERTYPE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 5));
            add(new ColumnDescriptor("SUPERTYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 6));
        }
    };

    /*
        TABLE_CAT String => the type's catalog (may be null)
        TABLE_SCHEM String => type's schema (may be null)
        TABLE_NAME String => type name
        SUPERTABLE_NAME String => the direct super type's name
     */
    public static final List<ColumnDescriptor> SUPER_TABLES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TYPE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("SUPERTABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
        }
    };

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
    public static final List<ColumnDescriptor> ATTRIBUTES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TYPE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TYPE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("ATTR_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 5));
            add(new ColumnDescriptor("ATTR_TYPE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 6));
            add(new ColumnDescriptor("ATTR_SIZE", new ColumnTypeDescriptor(HiveType.INTEGER), 7));
            add(new ColumnDescriptor("DECIMAL_DIGITS", new ColumnTypeDescriptor(HiveType.INTEGER), 8));
            add(new ColumnDescriptor("NUM_PREC_RADIX", new ColumnTypeDescriptor(HiveType.INTEGER), 9));
            add(new ColumnDescriptor("NULLABLE", new ColumnTypeDescriptor(HiveType.INTEGER), 10));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 11));
            add(new ColumnDescriptor("ATTR_DEF", new ColumnTypeDescriptor(HiveType.STRING), 12));
            add(new ColumnDescriptor("SQL_DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 13));
            add(new ColumnDescriptor("SQL_DATETIME_SUB", new ColumnTypeDescriptor(HiveType.INTEGER), 14));
            add(new ColumnDescriptor("CHAR_OCTET_LENGTH", new ColumnTypeDescriptor(HiveType.INTEGER), 15));
            add(new ColumnDescriptor("ORDINAL_POSITION", new ColumnTypeDescriptor(HiveType.INTEGER), 16));
            add(new ColumnDescriptor("IS_NULLABLE", new ColumnTypeDescriptor(HiveType.STRING), 17));
            add(new ColumnDescriptor("SCOPE_CATALOG", new ColumnTypeDescriptor(HiveType.STRING), 18));
            add(new ColumnDescriptor("SCOPE_SCHEMA", new ColumnTypeDescriptor(HiveType.STRING), 19));
            add(new ColumnDescriptor("SCOPE_TABLE", new ColumnTypeDescriptor(HiveType.STRING), 20));
            add(new ColumnDescriptor("SOURCE_DATA_TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 21));
        }
    };

    /*
        NAME String=> The name of the client info property
        MAX_LEN int=> The maximum length of the value for the property
        DEFAULT_VALUE String=> The default value of the property
        DESCRIPTION String=> A description of the property. This will typically contain information as to where this property is stored in the database.
     */
    public static final List<ColumnDescriptor> CLIENT_INFO_PROPERTIES = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("NAME", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("MAX_LEN", new ColumnTypeDescriptor(HiveType.INTEGER), 2));
            add(new ColumnDescriptor("DEFAULT_VALUE", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("DESCRIPTION", new ColumnTypeDescriptor(HiveType.STRING), 4));
        }
    };

    /*
        FUNCTION_CAT String => function catalog (may be null)
        FUNCTION_SCHEM String => function schema (may be null)
        FUNCTION_NAME String => function name. This is the name used to invoke the function
        REMARKS String => explanatory comment on the function
        FUNCTION_TYPE short => kind of function:
        SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload functions
     */
    public static final List<ColumnDescriptor> FUNCTION_COLUMNS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("FUNCTION_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("FUNCTION_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("FUNCTION_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("FUNCTION_TYPE", new ColumnTypeDescriptor(HiveType.SMALL_INT), 5));
            add(new ColumnDescriptor("SPECIFIC_NAME", new ColumnTypeDescriptor(HiveType.STRING), 6));
        }
    };

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
    public static final List<ColumnDescriptor> PSEUDO_COLUMNS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("TABLE_CAT", new ColumnTypeDescriptor(HiveType.STRING), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", new ColumnTypeDescriptor(HiveType.STRING), 2));
            add(new ColumnDescriptor("TABLE_NAME", new ColumnTypeDescriptor(HiveType.STRING), 3));
            add(new ColumnDescriptor("COLUMN_NAME", new ColumnTypeDescriptor(HiveType.STRING), 4));
            add(new ColumnDescriptor("DATA_TYPE", new ColumnTypeDescriptor(HiveType.INTEGER), 5));
            add(new ColumnDescriptor("COLUMN_SIZE", new ColumnTypeDescriptor(HiveType.INTEGER), 6));
            add(new ColumnDescriptor("DECIMAL_DIGITS", new ColumnTypeDescriptor(HiveType.INTEGER), 7));
            add(new ColumnDescriptor("NUM_PREC_RADIX", new ColumnTypeDescriptor(HiveType.INTEGER), 8));
            add(new ColumnDescriptor("COLUMN_USAGE", new ColumnTypeDescriptor(HiveType.STRING), 9));
            add(new ColumnDescriptor("REMARKS", new ColumnTypeDescriptor(HiveType.STRING), 10));
            add(new ColumnDescriptor("CHAR_OCTET_LENGTH", new ColumnTypeDescriptor(HiveType.INTEGER), 11));
            add(new ColumnDescriptor("IS_NULLABLE", new ColumnTypeDescriptor(HiveType.STRING), 12));
        }
    };

    public static final List<ColumnDescriptor> GENERATED_KEYS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("GENERATED_KEY", new ColumnTypeDescriptor(HiveType.STRING), 1));
        }
    };
}
