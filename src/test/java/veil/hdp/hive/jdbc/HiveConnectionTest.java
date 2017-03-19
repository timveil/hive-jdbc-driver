package veil.hdp.hive.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

public class HiveConnectionTest extends BaseJunitTest {

    Connection connection = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");
        properties.setProperty("hive.server2.transport.mode", "http");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default?transport.mode=binary";

        connection = new HiveDriver().connect(url, properties);

    }

    @After
    public void tearDown() throws Exception {

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void createStatement() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from test_table");

        ResultSetMetaData metaData = rs.getMetaData();

        while (rs.next()) {

            byte colTinyInt = rs.getByte("col_tinyint");
            log.debug("colTinyInt [{}]", colTinyInt);

            short colSmallInt = rs.getShort("col_smallint");
            log.debug("colSmallInt [{}]", colSmallInt);

            int colInt = rs.getInt("col_int");
            log.debug("colInt [{}]", colInt);

            long colBigint = rs.getLong("col_bigint");
            log.debug("colBigint [{}]", colBigint);

            boolean colBoolean = rs.getBoolean("col_boolean");
            log.debug("colBoolean [{}]", colBoolean);

            float colFloat = rs.getFloat("col_float");
            log.debug("colFloat [{}]", colFloat);

            double colDouble = rs.getDouble("col_double");
            log.debug("colDouble [{}]", colDouble);

            String colString = rs.getString("col_string");
            log.debug("colString [{}]", colString);

            Timestamp colTimestamp = rs.getTimestamp("col_timestamp");
            log.debug("colTimestamp [{}]", colTimestamp);

            BigDecimal colDecimal = rs.getBigDecimal("col_decimal");
            log.debug("colDecimal [{}]", colDecimal);

            String colVarchar = rs.getString("col_varchar");
            log.debug("colVarchar [{}]", colVarchar);

            Date colDate = rs.getDate("col_date");
            log.debug("colDate [{}]", colDate);

            String colChar = rs.getString("col_char");
            log.debug("colChar [{}]", colChar);

            Object colBinary = rs.getObject("col_binary");
            log.debug("colBinary [{}]", colBinary);


        }

    }

    @Test
    public void getMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: {}", metaData.getDriverVersion());
        log.debug("database product version: {}", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: {}", metaData.supportsTransactions());

        ResultSet catalogs = metaData.getCatalogs();

        metaData.supportsAlterTableWithAddColumn();

        if (catalogs != null) {
            while (catalogs.next()) {
                log.debug("TABLE_CAT {}", catalogs.getString("TABLE_CAT"));
            }
        }

        ResultSet schemas = metaData.getSchemas();

        if (schemas != null) {
            while (schemas.next()) {
                log.debug("TABLE_SCHEM {}", schemas.getString("TABLE_SCHEM"));
                log.debug("TABLE_CATALOG {}", schemas.getString("TABLE_CATALOG"));
            }
        }

        ResultSet typeInfo = metaData.getTypeInfo();

        if (typeInfo != null) {
            while (typeInfo.next()) {
                log.debug("TYPE_NAME {}", typeInfo.getString("TYPE_NAME"));
            }
        }

        ResultSet tables = metaData.getTables(null, null, null, null);

        if (tables != null) {
            while (tables.next()) {
                log.debug("TABLE_NAME {}", tables.getString("TABLE_NAME"));
            }
        }


        ResultSet tableTypes = metaData.getTableTypes();

        if (tableTypes != null) {
            while (tableTypes.next()) {
                log.debug("TABLE_TYPE {}", tableTypes.getString("TABLE_TYPE"));
            }
        }


        ResultSet columns = metaData.getColumns(null, null, null, null);

        if (columns != null) {
            while (columns.next()) {
                log.debug("TABLE_CAT {}", columns.getString("TABLE_CAT"));
                log.debug("TABLE_SCHEM {}", columns.getString("TABLE_SCHEM"));
                log.debug("TABLE_NAME {}", columns.getString("TABLE_NAME"));
                log.debug("COLUMN_NAME {}", columns.getString("COLUMN_NAME"));
                log.debug("DATA_TYPE {}", columns.getInt("DATA_TYPE"));
                log.debug("TYPE_NAME {}", columns.getString("TYPE_NAME"));
                log.debug("COLUMN_SIZE {}", columns.getInt("COLUMN_SIZE"));
                log.debug("BUFFER_LENGTH {}", columns.getByte("BUFFER_LENGTH"));
                log.debug("DECIMAL_DIGITS {}", columns.getInt("DECIMAL_DIGITS"));
                log.debug("NUM_PREC_RADIX {}", columns.getInt("NUM_PREC_RADIX"));
                log.debug("NULLABLE {}", columns.getInt("NULLABLE"));
                log.debug("REMARKS {}", columns.getString("REMARKS"));
                log.debug("COLUMN_DEF {}", columns.getString("COLUMN_DEF"));
                log.debug("SQL_DATA_TYPE {}", columns.getInt("SQL_DATA_TYPE"));
                log.debug("SQL_DATETIME_SUB {}", columns.getInt("SQL_DATETIME_SUB"));
                log.debug("CHAR_OCTET_LENGTH {}", columns.getInt("CHAR_OCTET_LENGTH"));
                log.debug("ORDINAL_POSITION {}", columns.getInt("ORDINAL_POSITION"));
                log.debug("IS_NULLABLE {}", columns.getBoolean("IS_NULLABLE"));
                log.debug("SCOPE_CATALOG {}", columns.getString("SCOPE_CATALOG"));
                log.debug("SCOPE_SCHEMA {}", columns.getString("SCOPE_SCHEMA"));
                log.debug("SCOPE_TABLE {}", columns.getString("SCOPE_TABLE"));
                log.debug("SOURCE_DATA_TYPE {}", columns.getShort("SOURCE_DATA_TYPE"));
                log.debug("IS_AUTO_INCREMENT {}", columns.getBoolean("IS_AUTO_INCREMENT"));
            }
        }
    }


}