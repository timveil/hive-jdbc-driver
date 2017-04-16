package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TStatus;
import veil.hdp.hive.jdbc.utils.HiveExceptionUtils;

import java.sql.SQLException;

public class HiveSQLException extends SQLException {

    public HiveSQLException(TStatus status) {

        super(status.getErrorMessage(), status.getSqlState(), status.getErrorCode());

        if (status.getInfoMessages() != null) {
            initCause(HiveExceptionUtils.toStackTrace(status.getInfoMessages()));
        }

    }

    public HiveSQLException(Throwable cause) {
        super(cause);
    }

    public HiveSQLException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public HiveSQLException(String reason) {
        super(reason);
    }

    public HiveSQLException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }
}
