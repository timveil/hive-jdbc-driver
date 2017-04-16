package veil.hdp.hive.jdbc;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class BaseConnectionTest extends BaseUnitTest {

    private static final MetricRegistry metrics = new MetricRegistry();
    private Connection connection;

    public abstract Connection createConnection(String host) throws SQLException;

    @Before
    public void setUp() throws Exception {
        connection = createConnection(getHost());
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
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
    public void testSimpleQuery() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_table")) {

            Printer.printResultSet(rs);
        }


    }

    @Test
    public void testPreparedStatement() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM test_table where col_string = ?")) {
            statement.setString(1, "test");

            try (ResultSet rs = statement.executeQuery()) {

                Printer.printResultSet(rs);
            }
        }


    }

    @Test
    public void testSimpleQueryLoad() throws SQLException {

        Timer timer = metrics.timer(MetricRegistry.name(this.getClass(), "testSimpleQueryLoad"));

        try (ConsoleReporter reporter = ConsoleReporter
                .forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()) {

            for (int i = 0; i < getTestRuns(); i++) {
                log.debug("run # {}", i);

                try (Timer.Context queryContext = timer.time()) {
                    testSimpleQuery();
                    queryContext.stop();
                }
            }


            reporter.report();
        }

    }

    @Test
    public void testPreparedStatementLoad() throws SQLException {
        Timer timer = metrics.timer(MetricRegistry.name(this.getClass(), "testPreparedStatementLoad"));

        try (ConsoleReporter reporter = ConsoleReporter
                .forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()) {

            for (int i = 0; i < getTestRuns(); i++) {
                log.debug("run # {}", i);

                try (Timer.Context queryContext = timer.time()) {
                    testPreparedStatement();
                    queryContext.stop();
                }
            }


            reporter.report();
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
                    Printer.printResultSet(rs);
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
    public void testColumnTypes() throws SQLException {
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

                    Printer.printResultSet(columns);
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
    public void testGetMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        Assert.assertNotNull(metaData);

        log.debug("driver version: [{}]", metaData.getDriverVersion());
        log.debug("database product version: [{}]", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: [{}]", metaData.supportsTransactions());


        log.debug("******************************** calling getCatalogs");

        try (ResultSet catalogs = metaData.getCatalogs()) {

            Printer.printResultSet(catalogs);
        }

        log.debug("******************************** calling getSchemas");

        try (ResultSet schemas = metaData.getSchemas()) {

            Printer.printResultSet(schemas);
        }

        log.debug("******************************** calling getTypeInfo");

        try (ResultSet typeInfo = metaData.getTypeInfo()) {

            Printer.printResultSet(typeInfo);
        }

        log.debug("******************************** calling getTables");

        try (ResultSet tables = metaData.getTables(null, null, null, null)) {

            Printer.printResultSet(tables);
        }

        log.debug("******************************** calling getTableTypes");

        try (ResultSet tableTypes = metaData.getTableTypes()) {

            Printer.printResultSet(tableTypes);
        }

        log.debug("******************************** calling getFunctions");

        try (ResultSet functions = metaData.getFunctions(null, null, "%")) {

            Printer.printResultSet(functions);
        }

        log.debug("******************************** calling getColumns");

        try (ResultSet columns = metaData.getColumns(null, "default", "test_table", "%")) {

            Printer.printResultSet(columns);
        }
    }


}
