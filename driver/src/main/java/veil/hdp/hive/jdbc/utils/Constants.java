package veil.hdp.hive.jdbc.utils;

public final class Constants {

    public static final int DEFAULT_MAX_ROWS = 0;
    public static final int DEFAULT_QUERY_TIMEOUT = 0;

    public static final int JDBC_MAJOR_VERSION = 4;
    public static final int JDBC_MINOR_VERSION = 2;

    public static final char SEARCH_STRING_ESCAPE = '\\';
    public static final char IDENTIFIER_QUOTE_STRING = '`';
    public static final char CATALOG_SEPARATOR = '.';
    public static final String CATALOG_TERM = "instance";
    public static final String SCHEMA_TERM = "database";

    public static final String SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
    public static final String JAVAX_SECURITY_AUTH_USE_SUBJECT_CREDS_ONLY = "javax.security.auth.useSubjectCredsOnly";

    private Constants() {
    }
}
