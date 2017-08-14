package veil.hdp.hive.jdbc.utils;

import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.SQLException;
import java.text.MessageFormat;

public final class ResultSetUtils {

    private ResultSetUtils() {
    }

    public static int findColumnIndex(Schema schema, String columnLabel) throws SQLException {
        ColumnDescriptor columnDescriptor = schema.getColumn(columnLabel);

        if (columnDescriptor != null) {
            return columnDescriptor.getPosition();
        }

        throw new HiveSQLException(MessageFormat.format("Could not find column for name {0} in Schema {1}", columnLabel, schema));
    }

}
