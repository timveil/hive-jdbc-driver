/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
