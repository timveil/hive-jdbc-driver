package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.utils.HiveExceptionUtils;

import java.sql.SQLException;

public class HiveThriftException extends SQLException {

    public HiveThriftException(TStatus status) {

        super(status.getErrorMessage(), status.getSqlState(), status.getErrorCode());

        if (status.getInfoMessages() != null) {
            initCause(HiveExceptionUtils.toStackTrace(status.getInfoMessages()));
        }

    }

    public HiveThriftException(TException exception) {
        super(exception);
    }

    public HiveThriftException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
