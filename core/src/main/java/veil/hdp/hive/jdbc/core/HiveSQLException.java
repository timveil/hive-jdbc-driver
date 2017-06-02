package veil.hdp.hive.jdbc.core;

import java.sql.SQLException;

public class HiveSQLException extends SQLException {
    public HiveSQLException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public HiveSQLException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public HiveSQLException(String reason) {
        super(reason);
    }

    public HiveSQLException() {
    }

    public HiveSQLException(Throwable cause) {
        super(cause);
    }

    public HiveSQLException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public HiveSQLException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public HiveSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }
}
