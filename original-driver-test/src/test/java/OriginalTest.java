import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hive.jdbc.HiveDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class OriginalTest {

    private final Logger log = LoggerFactory.getLogger(getClass());


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

            log.debug("attempting to close from tear down");
            connection.close();
        }
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


    private void printResultSet(ResultSet rs) {


        try {
            ResultSetMetaData metaData = rs.getMetaData();


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
