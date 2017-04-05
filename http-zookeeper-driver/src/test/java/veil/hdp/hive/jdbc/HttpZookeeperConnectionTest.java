package veil.hdp.hive.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HttpZookeeperConnectionTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    Connection connection = null;

    @Before
    public void setUp() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://hive.hdp.local:10000/default";

        //connection = new org.apache.hive.jdbc.HiveDriver().connect(url, properties);
        connection = new HttpZookeeperHiveDriver().connect(url, properties);

    }

    @After
    public void tearDown() throws Exception {

        if (connection != null) {

            log.debug("attempting to close from tear down");
            connection.close();
        }
    }

    @Test
    public void getSchema() throws SQLException {
        String schema = connection.getSchema();

        log.debug("schema [{}]", schema);
    }

    @Test
    public void setSchema() throws SQLException {
        connection.setSchema("default");
    }

    @Test
    public void abort() throws SQLException {
        connection.abort(Executors.newSingleThreadExecutor());
    }

    @Test
    public void isValid() throws SQLException {
        log.debug("is valid {}", connection.isValid(1));

        connection.close();

        log.debug("is valid {}", connection.isValid(1));
    }

    @Test
    public void createStatement2() throws SQLException {
        Statement statement = connection.createStatement();
        //statement.setFetchSize(2);
        //statement.setMaxRows(5);
        ResultSet rs = statement.executeQuery("SELECT * FROM hivetest.master");
        printResultSet(rs);

        rs.close();
        statement.close();

    }

    @Test
    public void load() throws SQLException {
        for (int i = 0; i < 1000; i++) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT * FROM test_table")) {
                while (rs.next()) {
                    printResultSet(rs);
                }
                log.debug("run # {}", i);
            }
        }
    }

    @Test
    public void testLoadWithConcurrency() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Runnable test = () -> {
            log.debug("******************************** calling getColumns");

            try {
                try (Statement statement = connection.createStatement();
                     ResultSet rs = statement.executeQuery("SELECT * FROM test_table")) {
                    while (rs.next()) {
                        //no-op
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };

        for (int i = 0; i < 1000; i++) {
            executorService.submit(test);
        }

        log.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ calling awaitTermination ");

        executorService.awaitTermination(2, TimeUnit.MINUTES);

    }


    @Test
    public void createStatement() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_table")) {

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

    }

    @Test
    public void testConcurrency() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Runnable test = () -> {
            log.debug("******************************** calling getColumns");

            try {
                try (ResultSet columns = metaData.getColumns(null, "default", "test_table", "%")) {

                    printResultSet(columns);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };

        for (int i = 0; i < 10; i++) {
            executorService.submit(test);
        }

        log.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ calling awaitTermination ");

        executorService.awaitTermination(10, TimeUnit.SECONDS);

    }


    @Test
    public void getMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: [{}]", metaData.getDriverVersion());
        log.debug("database product version: [{}]", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: [{}]", metaData.supportsTransactions());


        log.debug("******************************** calling getCatalogs");

        try (ResultSet catalogs = metaData.getCatalogs()) {

            printResultSet(catalogs);
        }

        log.debug("******************************** calling getSchemas");

        try (ResultSet schemas = metaData.getSchemas()) {

            printResultSet(schemas);
        }

        log.debug("******************************** calling getTypeInfo");

        try (ResultSet typeInfo = metaData.getTypeInfo()) {

            printResultSet(typeInfo);
        }

        log.debug("******************************** calling getTables");

        try (ResultSet tables = metaData.getTables(null, null, null, null)) {

            printResultSet(tables);
        }

        log.debug("******************************** calling getTableTypes");

        try (ResultSet tableTypes = metaData.getTableTypes()) {

            printResultSet(tableTypes);
        }

        log.debug("******************************** calling getFunctions");

        try (ResultSet functions = metaData.getFunctions(null, null, "%")) {

            printResultSet(functions);
        }

        log.debug("******************************** calling getColumns");

        try (ResultSet columns = metaData.getColumns(null, "default", "test_table", "%")) {

            printResultSet(columns);
        }
    }


    private void printResultSetMetaData(ResultSetMetaData rsmd) {

        log.debug("printing ResultSetMetaData metadata");

        try {
            int columnCount = rsmd.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                StringBuilder builder = new StringBuilder();
                //builder.append("table name [").append(rsmd.getTableName(i + 1)).append("], ");
                //builder.append("catalog name [").append(rsmd.getCatalogName(i + 1)).append("], ");
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
                //builder.append("isDefinitelyWritable [").append(rsmd.isDefinitelyWritable(i + 1)).append("], ");
                builder.append("isNullable [").append(rsmd.isNullable(i + 1)).append("], ");
                builder.append("isReadOnly [").append(rsmd.isReadOnly(i + 1)).append("], ");
                //builder.append("isSearchable [").append(rsmd.isSearchable(i + 1)).append("], ");
                //builder.append("isSigned [").append(rsmd.isSigned(i + 1)).append("], ");
                //builder.append("isWritable [").append(rsmd.isWritable(i + 1)).append("]");

                log.debug(builder.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void printResultSet(ResultSet rs) {


        try {
            ResultSetMetaData metaData = rs.getMetaData();

            //printResultSetMetaData(metaData);

            int columnCount = metaData.getColumnCount();


            log.debug("printing ResultSet");

            int counter = 0;

            while (rs.next()) {

                List<String> row = Lists.newArrayList();


                for (int i = 0; i < columnCount; i++) {

                    String columnName = metaData.getColumnName(i + 1);
                    String columnValue = rs.getString(i + 1);

                    row.add(columnName + " [" + columnValue + "]");

                }


                String join = Joiner.on(",").join(row);

                log.debug("{} - {}", counter++, join);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}