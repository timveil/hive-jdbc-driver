package veil.hdp.hive.jdbc.thrift;

import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.bindings.TGetOperationStatusResp;
import veil.hdp.hive.jdbc.bindings.TStatus;
import veil.hdp.hive.jdbc.utils.HiveExceptionUtils;


public class HiveThriftException extends RuntimeException {


    public HiveThriftException(TException cause) {
        super(cause);
    }


    public HiveThriftException(TGetOperationStatusResp operationStatusResp) {
        super();

        // todo - need to understand whats available here
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + operationStatusResp);
    }

    public HiveThriftException(TStatus status) {

        super(status.getErrorMessage());

        if (status.getInfoMessages() != null) {
            initCause(HiveExceptionUtils.toStackTrace(status.getInfoMessages()));
        }

    }

}
