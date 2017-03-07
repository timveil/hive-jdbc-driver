package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;

/**
 * Created by timve on 3/6/2017.
 */
public interface OperationHandleCallback {

    void process(TOperationHandle statementHandle);
}
