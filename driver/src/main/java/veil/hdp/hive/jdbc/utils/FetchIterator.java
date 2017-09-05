package veil.hdp.hive.jdbc.utils;

import com.google.common.collect.AbstractIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.bindings.TFetchOrientation;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.thrift.ThriftOperation;

public class FetchIterator extends AbstractIterator<ColumnBasedSet> {

    private static final Logger log = LogManager.getLogger(FetchIterator.class);

    private final ThriftOperation operation;
    private final int fetchSize;

    public FetchIterator(ThriftOperation operation, int fetchSize) {
        this.operation = operation;
        this.fetchSize = fetchSize;
    }

    @Override
    protected ColumnBasedSet computeNext() {

        ColumnBasedSet cbs = ThriftUtils.fetchResults(operation, TFetchOrientation.FETCH_NEXT, fetchSize);

        if (cbs != null && cbs.getRowCount() > 0) {
            return cbs;
        } else {
            return endOfData();
        }

    }
}
