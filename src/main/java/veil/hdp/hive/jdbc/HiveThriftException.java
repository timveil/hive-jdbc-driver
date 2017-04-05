package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.thrift.TException;

import java.sql.SQLException;

public class HiveThriftException extends SQLException {

    public HiveThriftException(TStatus status) {
        //todo; got to fix this
    }

    public HiveThriftException(TException exception) {

        super(exception);


    }

}
