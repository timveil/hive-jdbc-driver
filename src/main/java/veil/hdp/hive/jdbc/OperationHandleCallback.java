package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TOperationHandle;

public interface OperationHandleCallback {

    void process(TOperationHandle statementHandle);
}
