package veil.hdp.hive.jdbc.core.thrift;

import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.core.binding.TGetOperationStatusResp;
import veil.hdp.hive.jdbc.core.binding.TStatus;
import veil.hdp.hive.jdbc.core.utils.HiveExceptionUtils;


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
