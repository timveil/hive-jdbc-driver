package veil.hdp.hive.jdbc.core.utils;

import org.slf4j.Logger;
import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.core.metadata.Schema;

import java.sql.SQLException;
import java.text.MessageFormat;

import static org.slf4j.LoggerFactory.getLogger;

public class ResultSetUtils {

    private static final Logger log = getLogger(ResultSetUtils.class);

    public static int findColumnIndex(Schema schema, String columnLabel) throws SQLException {
        ColumnDescriptor columnDescriptor = schema.getColumn(columnLabel);

        if (columnDescriptor != null) {
            return columnDescriptor.getPosition();
        }

        throw new HiveSQLException(MessageFormat.format("Could not find column for name {0} in Schema {1}", columnLabel, schema));
    }

}
