package veil.hdp.hive.jdbc.thrift;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.bindings.TGetOperationStatusResp;
import veil.hdp.hive.jdbc.bindings.TStatus;
import veil.hdp.hive.jdbc.utils.HiveExceptionUtils;


public class HiveThriftException extends RuntimeException {

    private static final Logger log = LoggerFactory.getLogger(HiveThriftException.class);
    private static final long serialVersionUID = 1700514420277606047L;


    public HiveThriftException(TException cause) {
        super(cause);
    }


    public HiveThriftException(TGetOperationStatusResp operationStatusResp) {
        super();

        // todo - need to understand whats available here
        log.warn("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + operationStatusResp);
    }

    public HiveThriftException(TStatus status) {

        super(status.getErrorMessage());

        if (status.getInfoMessages() != null) {
            initCause(HiveExceptionUtils.toStackTrace(status.getInfoMessages()));
        }

    }

}
