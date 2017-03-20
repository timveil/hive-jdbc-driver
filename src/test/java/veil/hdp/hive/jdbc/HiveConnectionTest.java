package veil.hdp.hive.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class HiveConnectionTest extends BaseJunitTest {

    Connection connection = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive-large.hdp.local:10000/default";

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
        ResultSet rs = statement.executeQuery("SELECT * FROM test_table");

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

            Object colBinary = rs.getBytes("col_binary");
            log.debug("colBinary [{}]", colBinary);

            byte[] stringAsBytes = rs.getBytes("col_string");
            log.debug("col_string as bytes [{}]", stringAsBytes);

            InputStream stringAsStream = rs.getBinaryStream("col_string");
            log.debug("col_string as InputStream [{}]", stringAsStream);

        }

    }

    @Test
    public void getMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: [{}]", metaData.getDriverVersion());
        log.debug("database product version: [{}]", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: [{}]", metaData.supportsTransactions());

        log.debug("******************************** calling getCatalogs");

        ResultSet catalogs = metaData.getCatalogs();

        printResultSet(catalogs);

        log.debug("******************************** calling getSchemas");

        ResultSet schemas = metaData.getSchemas();

        printResultSet(schemas);

        log.debug("******************************** calling getTypeInfo");

        ResultSet typeInfo = metaData.getTypeInfo();

        printResultSet(typeInfo);

        log.debug("******************************** calling getTables");

        ResultSet tables = metaData.getTables(null, null, null, null);

        printResultSet(tables);

        log.debug("******************************** calling getTableTypes");

        ResultSet tableTypes = metaData.getTableTypes();

        printResultSet(tableTypes);

        log.debug("******************************** calling getFunctions");

        ResultSet functions = metaData.getFunctions(null, null, "%");

        printResultSet(functions);

        log.debug("******************************** calling getColumns");

        ResultSet columns = metaData.getColumns(null, "default", "test_table", "%");

        printResultSet(columns);
    }


    private void printResultSetMetaData(ResultSetMetaData rsmd) {

        log.debug("printing ResultSetMetaData metadata");

        try {
            int columnCount = rsmd.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                StringBuilder builder = new StringBuilder();
                builder.append("table name [").append(rsmd.getTableName(i + 1)).append("], ");
                builder.append("catalog name [").append(rsmd.getCatalogName(i + 1)).append("], ");
                builder.append("column class name [").append(rsmd.getColumnClassName(i + 1)).append("], ");
                builder.append("column display size [").append(rsmd.getColumnDisplaySize(i + 1)).append("], ");
                builder.append("column label [").append(rsmd.getColumnLabel(i + 1)).append("], ");
                builder.append("column name [").append(rsmd.getColumnName(i + 1)).append("], ");
                builder.append("column type [").append(rsmd.getColumnType(i + 1)).append("], ");
                builder.append("column type name [").append(rsmd.getColumnTypeName(i + 1)).append("], ");
                builder.append("precision [").append(rsmd.getPrecision(i + 1)).append("], ");
                builder.append("getScale [").append(rsmd.getScale(i + 1)).append("], ");
                builder.append("isAutoIncrement [").append(rsmd.isAutoIncrement(i + 1)).append("], ");
                builder.append("isCaseSensitive [").append(rsmd.isCaseSensitive(i + 1)).append("], ");
                builder.append("isCurrency [").append(rsmd.isCurrency(i + 1)).append("], ");
                builder.append("isDefinitelyWritable [").append(rsmd.isDefinitelyWritable(i + 1)).append("], ");
                builder.append("isNullable [").append(rsmd.isNullable(i + 1)).append("], ");
                builder.append("isReadOnly [").append(rsmd.isReadOnly(i + 1)).append("], ");
                builder.append("isSearchable [").append(rsmd.isSearchable(i + 1)).append("], ");
                builder.append("isSigned [").append(rsmd.isSigned(i + 1)).append("], ");
                builder.append("isWritable [").append(rsmd.isWritable(i + 1)).append("]");

                log.debug(builder.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void printResultSet(ResultSet rs) {



        try {
            ResultSetMetaData metaData = rs.getMetaData();

            printResultSetMetaData(metaData);

            int columnCount = metaData.getColumnCount();


            log.debug("printing ResultSet");

            while (rs.next()) {

                List<String> row = Lists.newArrayList();


                for (int i = 0; i < columnCount; i++) {

                    String columnName = metaData.getColumnName(i + 1);
                    String columnValue = rs.getString(i + 1);

                    row.add(columnName + " [" + columnValue + "]");

                }


                String join = Joiner.on(",").join(row);

                log.debug(join);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}