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
    private final TFetchOrientation orientation;

    public FetchIterator(ThriftOperation operation, TFetchOrientation orientation, int fetchSize) {
        this.operation = operation;
        this.orientation = orientation;
        this.fetchSize = fetchSize;
    }

    @Override
    protected ColumnBasedSet computeNext() {

        ColumnBasedSet cbs = ThriftUtils.fetchResults(operation, orientation, fetchSize);

        if (cbs != null && cbs.getRowCount() > 0) {
            return cbs;
        } else {
            return endOfData();
        }

    }
}
