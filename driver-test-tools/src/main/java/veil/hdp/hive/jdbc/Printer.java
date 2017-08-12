package veil.hdp.hive.jdbc;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class Printer {
    private static final Logger log = LoggerFactory.getLogger(Printer.class);


    static void printResultSetMetaData(ResultSetMetaData rsmd) {

        try {
            int columnCount = rsmd.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                String metadata = "column class name [" + rsmd.getColumnClassName(i + 1) + "], " +
                        "column display size [" + rsmd.getColumnDisplaySize(i + 1) + "], " +
                        "column label [" + rsmd.getColumnLabel(i + 1) + "], " +
                        "column name [" + rsmd.getColumnName(i + 1) + "], " +
                        "table name [" + rsmd.getTableName(i + 1) + "], " +
                        "column type [" + rsmd.getColumnType(i + 1) + "], " +
                        "column type name [" + rsmd.getColumnTypeName(i + 1) + "], " +
                        "precision [" + rsmd.getPrecision(i + 1) + "], " +
                        "getScale [" + rsmd.getScale(i + 1) + "], " +
                        "isAutoIncrement [" + rsmd.isAutoIncrement(i + 1) + "], " +
                        "isCaseSensitive [" + rsmd.isCaseSensitive(i + 1) + "], " +
                        "isCurrency [" + rsmd.isCurrency(i + 1) + "], " +
                        "isNullable [" + rsmd.isNullable(i + 1) + "], " +
                        "isReadOnly [" + rsmd.isReadOnly(i + 1) + "], ";

                log.debug(metadata);
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }

    }

    static void printResultSet(ResultSet rs) {


        try {
            ResultSetMetaData metaData = rs.getMetaData();

            int columnCount = metaData.getColumnCount();
            int counter = 0;

            while (rs.next()) {

                List<String> row = new ArrayList<>();

                for (int i = 0; i < columnCount; i++) {

                    String columnName = metaData.getColumnName(i + 1);
                    String columnValue = rs.getString(i + 1);

                    row.add(columnName + " [" + columnValue + ']');

                }


                String join = Joiner.on(",").join(row);

                log.debug("row {} - data [{}]", counter, join);

                counter++;
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }

    }
}
