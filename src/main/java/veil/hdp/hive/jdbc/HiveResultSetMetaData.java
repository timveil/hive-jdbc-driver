package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.metadata.JdbcColumnAttributes;

import java.util.List;

/**
 * Created by timve on 3/6/2017.
 */
public class HiveResultSetMetaData extends AbstractResultSetMetaData {

    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final List<JdbcColumnAttributes> columnAttributes;

    public HiveResultSetMetaData(List<String> columnNames,
                                 List<String> columnTypes,
                                 List<JdbcColumnAttributes> columnAttributes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnAttributes = columnAttributes;
    }
}
