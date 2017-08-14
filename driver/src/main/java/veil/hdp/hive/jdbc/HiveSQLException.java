package veil.hdp.hive.jdbc;

import java.sql.SQLException;

public class HiveSQLException extends SQLException {
    private static final long serialVersionUID = -7971777991465574600L;


    public HiveSQLException(String reason) {
        super(reason);
    }

    public HiveSQLException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
