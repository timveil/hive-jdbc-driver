package veil.hdp.hive.jdbc.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class AbstractConnectionTest extends BaseTest {

    private static final int WARMUP = 10;
    private static final String SIMPLE_QUERY_LOAD = MetricRegistry.name(AbstractConnectionTest.class, "testSimpleQueryLoad");
    private static final String PREPARED_STATEMENT_LOAD = MetricRegistry.name(AbstractConnectionTest.class, "testPreparedStatementLoad");
    private static MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private Connection connection;

    public abstract Connection createConnection(String host) throws SQLException;

    @BeforeEach
    public void setUp() throws Exception {
        connection = createConnection(getHost());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } finally {
                connection = null;
            }
        }

        try {
            METRIC_REGISTRY.remove(SIMPLE_QUERY_LOAD);
            METRIC_REGISTRY.remove(PREPARED_STATEMENT_LOAD);
        } finally {
            METRIC_REGISTRY = null;
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
    public void testCancel() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM data_type_test limit 10")) {


            ExecutorService executorService = Executors.newFixedThreadPool(2);

            executorService.submit(() -> {

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }

                Printer.printResultSet(rs, true, true);
            });

            executorService.submit(() -> {
                try {
                    statement.cancel();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            });


            try {
                executorService.awaitTermination(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

        }
    }

    @Test
    public void isValid() throws SQLException {
        log.debug("is valid {}", connection.isValid(1));

        connection.close();

        log.debug("is valid {}", connection.isValid(1));
    }

    @Test
    public void testSimpleQuery() throws SQLException {
        executeSimpleQuery(true, true);
    }

    @Test
    public void testStructQuery() throws SQLException {
        executeStructQuery(true, true);
    }

    @Test
    public void testMapQuery() throws SQLException {
        executeMapQuery(true, true);
    }

    @Test
    public void testArrayQuery() throws SQLException {
        executeArrayQuery(true, true);
    }

    @Test
    public void testASQuery() throws SQLException {
        executeAsQuery(true, true);
    }

    @Test
    public void testDates() throws SQLException {

        //get Calendar instance
        Calendar now = Calendar.getInstance();

        //get current TimeZone using getTimeZone method of Calendar class
        TimeZone timeZone = now.getTimeZone();

        //display current TimeZone using getDisplayName() method of TimeZone class
        log.debug("Current TimeZone is: {}", timeZone.getDisplayName());

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("select col_date, col_timestamp from date_time_test")) {

            Printer.printResultSet(rs, true, false);
        }
    }

    private void executeAsQuery(boolean printResults, boolean printMetaData) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT col_string as `String Column` FROM data_type_test limit 10")) {

            Printer.printResultSet(rs, printResults, printMetaData);
        }
    }


    private void executeSimpleQuery(boolean printResults, boolean printMetaData) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM data_type_test")) {

            Printer.printResultSet(rs, printResults, printMetaData);
        }
    }

    private void executeStructQuery(boolean printResults, boolean printMetaData) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM struct_test")) {

            Printer.printResultSet(rs, printResults, printMetaData);
        }
    }


    private void executeMapQuery(boolean printResults, boolean printMetaData) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM map_test")) {

            Printer.printResultSet(rs, printResults, printMetaData);
        }
    }


    private void executeArrayQuery(boolean printResults, boolean printMetaData) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM array_test")) {

            Printer.printResultSet(rs, printResults, printMetaData);
        }
    }


    @Test
    public void testPreparedStatement() throws SQLException {
        executePreparedStatement(true, false);
    }

    private void executePreparedStatement(boolean printResults, boolean printMetaData) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM data_type_test where col_boolean = ? limit 10")) {
            statement.setBoolean(1, true);

            try (ResultSet rs = statement.executeQuery()) {

                Printer.printResultSet(rs, printResults, printMetaData);
            }
        }
    }

    @Test
    public void testSimpleQueryLoad() throws SQLException {

        // warm-up
        for (int i = 0; i < WARMUP; i++) {
            executeSimpleQuery(false, false);
        }


        try (ConsoleReporter reporter = ConsoleReporter
                .forRegistry(METRIC_REGISTRY)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()) {


            Timer timer = METRIC_REGISTRY.timer(SIMPLE_QUERY_LOAD);

            for (int i = 0; i < getTestRuns(); i++) {

                log.debug("------------------------------------- start run # {}", i);

                Stopwatch sw = Stopwatch.createStarted();

                try (Timer.Context queryContext = timer.time()) {
                    executeSimpleQuery(true, false);
                }

                sw.stop();

                log.debug("------------------------------------- end run # {}, took {}ms", i, sw.elapsed(TimeUnit.MILLISECONDS));
            }

            reporter.report();
        }

    }

    @Test
    public void testSimpleQueryInfiniteLoop() throws SQLException {
        while (true) {
            executeSimpleQuery(false, false);
        }
    }


    @Test
    public void testPreparedStatementLoad() throws SQLException {

        // warm-up
        for (int i = 0; i < WARMUP; i++) {
            executePreparedStatement(false, false);
        }


        try (ConsoleReporter reporter = ConsoleReporter
                .forRegistry(METRIC_REGISTRY)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()) {


            Timer timer = METRIC_REGISTRY.timer(PREPARED_STATEMENT_LOAD);

            for (int i = 0; i < getTestRuns(); i++) {
                log.debug("run # {}", i);

                try (Timer.Context queryContext = timer.time()) {
                    executePreparedStatement(true, false);
                }
            }

            reporter.report();
        }

    }

    @Test
    public void testLoadWithConcurrency() throws Exception {

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Runnable test = () -> {
            try {
                executeSimpleQuery(true, false);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        };

        for (int i = 0; i < getTestRuns(); i++) {
            executorService.submit(test);
        }

        executorService.awaitTermination(1, TimeUnit.MINUTES);

    }


    @Test
    public void testColumnTypes() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM data_type_test")) {

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

                byte[] stringAsBytes = rs.getBytes("col_string");
                log.debug("col_string as bytes [{}]", stringAsBytes);

                InputStream stringAsStream = rs.getBinaryStream("col_string");
                log.debug("col_string as InputStream [{}]", stringAsStream);

            }
        }

    }

    @Test
    public void testMetadataConcurrency() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Runnable test = () -> {
            try {
                try (ResultSet columns = metaData.getColumns(null, "tests", "data_type_test", "%")) {
                    Printer.printResultSet(columns, true, false);
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        };

        for (int i = 0; i < 10; i++) {
            executorService.submit(test);
        }

        executorService.awaitTermination(10, TimeUnit.SECONDS);

    }


    @Test
    public void testGetMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);

        log.debug("driver version: [{}]", metaData.getDriverVersion());
        log.debug("database product version: [{}]", metaData.getDatabaseProductVersion());
        log.debug("supports transactions: [{}]", metaData.supportsTransactions());


        log.debug("******************************** calling getCatalogs");

        try (ResultSet catalogs = metaData.getCatalogs()) {

            Printer.printResultSet(catalogs, true, false);
        }

        log.debug("******************************** calling getSchemas");

        try (ResultSet schemas = metaData.getSchemas()) {

            Printer.printResultSet(schemas, true, false);
        }

        log.debug("******************************** calling getTypeInfo");

        try (ResultSet typeInfo = metaData.getTypeInfo()) {

            Printer.printResultSet(typeInfo, true, false);
        }

        log.debug("******************************** calling getTables");

        try (ResultSet tables = metaData.getTables(null, null, null, null)) {

            Printer.printResultSet(tables, true, false);
        }

        log.debug("******************************** calling getTableTypes");

        try (ResultSet tableTypes = metaData.getTableTypes()) {

            Printer.printResultSet(tableTypes, true, false);
        }

        log.debug("******************************** calling getFunctions");

        try (ResultSet functions = metaData.getFunctions(null, null, "%")) {

            Printer.printResultSet(functions, true, false);
        }

        log.debug("******************************** calling getColumns");

        try (ResultSet columns = metaData.getColumns(null, "tests", "data_type_test", "%")) {

            Printer.printResultSet(columns, true, false);
        }
    }


}
