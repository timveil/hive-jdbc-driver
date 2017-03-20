package veil.hdp.hive.jdbc;


import org.apache.hive.service.cli.Type;

import java.util.ArrayList;
import java.util.List;

public class ColumnDescriptors {

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
            add(new ColumnDescriptor("TABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("TABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("KEY_SEQ", null, new TypeDescriptor(Type.INT_TYPE), 5));
            add(new ColumnDescriptor("PK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 6));
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
            add(new ColumnDescriptor("PROCEDURE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("PROCEDURE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("PROCEDURE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("RESERVED", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("RESERVED", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("RESERVED", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("REMARKS", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("PROCEDURE_TYPE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 8));
            add(new ColumnDescriptor("SPECIFIC_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 9));
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
            add(new ColumnDescriptor("PROCEDURE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("PROCEDURE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("PROCEDURE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("COLUMN_TYPE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 5));
            add(new ColumnDescriptor("DATA_TYPE", null, new TypeDescriptor(Type.INT_TYPE), 6));
            add(new ColumnDescriptor("TYPE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("PRECISION", null, new TypeDescriptor(Type.INT_TYPE), 8));
            add(new ColumnDescriptor("LENGTH", null, new TypeDescriptor(Type.INT_TYPE), 9));
            add(new ColumnDescriptor("SCALE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 10));
            add(new ColumnDescriptor("RADIX", null, new TypeDescriptor(Type.SMALLINT_TYPE), 11));
            add(new ColumnDescriptor("NULLABLE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 12));
            add(new ColumnDescriptor("REMARKS", null, new TypeDescriptor(Type.STRING_TYPE), 13));
            add(new ColumnDescriptor("COLUMN_DEF", null, new TypeDescriptor(Type.STRING_TYPE), 14));
            add(new ColumnDescriptor("SQL_DATA_TYPE", null, new TypeDescriptor(Type.INT_TYPE), 15));
            add(new ColumnDescriptor("SQL_DATETIME_SUB", null, new TypeDescriptor(Type.INT_TYPE), 16));
            add(new ColumnDescriptor("CHAR_OCTET_LENGTH", null, new TypeDescriptor(Type.INT_TYPE), 17));
            add(new ColumnDescriptor("ORDINAL_POSITION", null, new TypeDescriptor(Type.INT_TYPE), 18));
            add(new ColumnDescriptor("IS_NULLABLE", null, new TypeDescriptor(Type.STRING_TYPE), 19));
            add(new ColumnDescriptor("SPECIFIC_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 20));
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
            add(new ColumnDescriptor("TABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("TABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("GRANTOR", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("GRANTEE", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("PRIVILEGE", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("IS_GRANTABLE", null, new TypeDescriptor(Type.STRING_TYPE), 8));
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
            add(new ColumnDescriptor("SCOPE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 1));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("DATA_TYPE", null, new TypeDescriptor(Type.INT_TYPE), 3));
            add(new ColumnDescriptor("TYPE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("COLUMN_SIZE", null, new TypeDescriptor(Type.INT_TYPE), 5));
            add(new ColumnDescriptor("BUFFER_LENGTH", null, new TypeDescriptor(Type.INT_TYPE), 6));
            add(new ColumnDescriptor("DECIMAL_DIGITS", null, new TypeDescriptor(Type.SMALLINT_TYPE), 7));
            add(new ColumnDescriptor("PSEUDO_COLUMN", null, new TypeDescriptor(Type.SMALLINT_TYPE), 8));
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
            add(new ColumnDescriptor("TABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("TABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("GRANTOR", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("GRANTEE", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("PRIVILEGE", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("IS_GRANTABLE", null, new TypeDescriptor(Type.STRING_TYPE), 7));
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
    public static final List<ColumnDescriptor> VERSION_COLUMNS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("SCOPE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 1));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("DATA_TYPE", null, new TypeDescriptor(Type.INT_TYPE), 3));
            add(new ColumnDescriptor("TYPE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("COLUMN_SIZE", null, new TypeDescriptor(Type.INT_TYPE), 5));
            add(new ColumnDescriptor("BUFFER_LENGTH", null, new TypeDescriptor(Type.INT_TYPE), 6));
            add(new ColumnDescriptor("DECIMAL_DIGITS", null, new TypeDescriptor(Type.SMALLINT_TYPE), 7));
            add(new ColumnDescriptor("PSEUDO_COLUMN", null, new TypeDescriptor(Type.SMALLINT_TYPE), 8));
        }

    };

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
            add(new ColumnDescriptor("PKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("PKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("PKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("PKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("FKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("FKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("FKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("FKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 8));
            add(new ColumnDescriptor("KEY_SEQ", null, new TypeDescriptor(Type.SMALLINT_TYPE), 9));
            add(new ColumnDescriptor("UPDATE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 10));
            add(new ColumnDescriptor("DELETE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 11));
            add(new ColumnDescriptor("FK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 12));
            add(new ColumnDescriptor("PK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 13));
            add(new ColumnDescriptor("DEFERRABILITY", null, new TypeDescriptor(Type.SMALLINT_TYPE), 14));
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
    public static final List<ColumnDescriptor> EXPORTED_KEYS = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("PKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("PKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("PKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("PKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("FKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("FKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("FKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("FKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 8));
            add(new ColumnDescriptor("KEY_SEQ", null, new TypeDescriptor(Type.SMALLINT_TYPE), 9));
            add(new ColumnDescriptor("UPDATE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 10));
            add(new ColumnDescriptor("DELETE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 11));
            add(new ColumnDescriptor("FK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 12));
            add(new ColumnDescriptor("PK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 13));
            add(new ColumnDescriptor("DEFERRABILITY", null, new TypeDescriptor(Type.SMALLINT_TYPE), 14));
        }

    };

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
    public static final List<ColumnDescriptor> CROSS_REFERENCE = new ArrayList<ColumnDescriptor>() {
        {
            add(new ColumnDescriptor("PKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("PKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("PKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("PKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 4));
            add(new ColumnDescriptor("FKTABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("FKTABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("FKTABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 7));
            add(new ColumnDescriptor("FKCOLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 8));
            add(new ColumnDescriptor("KEY_SEQ", null, new TypeDescriptor(Type.SMALLINT_TYPE), 9));
            add(new ColumnDescriptor("UPDATE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 10));
            add(new ColumnDescriptor("DELETE_RULE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 11));
            add(new ColumnDescriptor("FK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 12));
            add(new ColumnDescriptor("PK_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 13));
            add(new ColumnDescriptor("DEFERRABILITY", null, new TypeDescriptor(Type.SMALLINT_TYPE), 14));
        }

    };

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
            add(new ColumnDescriptor("TABLE_CAT", null, new TypeDescriptor(Type.STRING_TYPE), 1));
            add(new ColumnDescriptor("TABLE_SCHEM", null, new TypeDescriptor(Type.STRING_TYPE), 2));
            add(new ColumnDescriptor("TABLE_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 3));
            add(new ColumnDescriptor("NON_UNIQUE", null, new TypeDescriptor(Type.BOOLEAN_TYPE), 4));
            add(new ColumnDescriptor("INDEX_QUALIFIER", null, new TypeDescriptor(Type.STRING_TYPE), 5));
            add(new ColumnDescriptor("INDEX_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 6));
            add(new ColumnDescriptor("TYPE", null, new TypeDescriptor(Type.SMALLINT_TYPE), 7));
            add(new ColumnDescriptor("ORDINAL_POSITION", null, new TypeDescriptor(Type.SMALLINT_TYPE), 8));
            add(new ColumnDescriptor("COLUMN_NAME", null, new TypeDescriptor(Type.STRING_TYPE), 9));
            add(new ColumnDescriptor("ASC_OR_DESC", null, new TypeDescriptor(Type.STRING_TYPE), 10));
            add(new ColumnDescriptor("CARDINALITY", null, new TypeDescriptor(Type.BIGINT_TYPE), 11));
            add(new ColumnDescriptor("PAGES", null, new TypeDescriptor(Type.BIGINT_TYPE), 12));
            add(new ColumnDescriptor("FILTER_CONDITION", null, new TypeDescriptor(Type.STRING_TYPE), 13));
        }

    };
}
