package veil.hdp.hive.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class Printer {
    private static final Logger log = LoggerFactory.getLogger(Printer.class);


    public static void printResultSetMetaData(ResultSetMetaData rsmd) {

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
            log.error(e.getMessage(), e);
        }

    }

    public static void printResultSet(ResultSet rs) {


        try {
            ResultSetMetaData metaData = rs.getMetaData();

            int columnCount = metaData.getColumnCount();
            int counter = 0;

            while (rs.next()) {

                List<String> row = Lists.newArrayList();

                for (int i = 0; i < columnCount; i++) {

                    String columnName = metaData.getColumnName(i + 1);
                    String columnValue = rs.getString(i + 1);

                    row.add(columnName + " [" + columnValue + "]");

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
