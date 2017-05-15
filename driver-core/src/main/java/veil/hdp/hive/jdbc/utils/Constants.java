package veil.hdp.hive.jdbc.utils;

import java.text.SimpleDateFormat;

public class Constants {

    public static final SimpleDateFormat HIVE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static final int DEFAULT_FETCH_SIZE = 1000;
    public static final int DEFAULT_MAX_ROWS = 0;
    public static final int DEFAULT_QUERY_TIMEOUT = 0;

    public static final int DRIVER_MAJOR_VERSION = 1;
    public static final int DRIVER_MINOR_VERSION = 0;

    public static final int JDBC_MAJOR_VERSION = 4;
    public static final int JDBC_MINOR_VERSION = 2;

    public static final String DATABASE_PRODUCT_NAME = "Apache Hive";

    public static final char SEARCH_STRING_ESCAPE = '\\';
    public static final char IDENTIFIER_QUOTE_STRING = '`';
    public static final char CATALOG_SEPARATOR = '.';
    public static final String CATALOG_TERM = "instance";
    public static final String SCHEMA_TERM = "database";
}
