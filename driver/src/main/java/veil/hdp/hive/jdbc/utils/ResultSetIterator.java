package veil.hdp.hive.jdbc.utils;

import com.google.common.collect.AbstractIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.data.Row;

import java.util.Iterator;

public class ResultSetIterator extends AbstractIterator<Row> {

    private static final Logger log = LogManager.getLogger(ResultSetIterator.class);

    private final Iterator<ColumnBasedSet> fetchIterator;
    private final int fetchSize;

    private int index = 0;
    private Iterator<Row> fetchRowIterator;

    public ResultSetIterator(Iterator<ColumnBasedSet> fetchIterator, int fetchSize) {
        this.fetchIterator = fetchIterator;
        this.fetchSize = fetchSize;
    }

    @Override
    protected Row computeNext() {

        while (true) {


            if (fetchRowIterator == null) {
                // no rows inside a page available; go get them

                if (fetchIterator.hasNext()) {
                    // there is another page avaialble, so create the row iterator
                    ColumnBasedSet cbs = fetchIterator.next();
                    fetchRowIterator = cbs.iterator();
                } else {
                    // no more pages, so its the end of the line
                    return endOfData();
                }

            }

            boolean moreRowsInPage = fetchRowIterator.hasNext();

            if (moreRowsInPage) {

                // the page has more results
                index++;

                return fetchRowIterator.next();

            } else {

                if (index < fetchSize) {

                    // the page has no more results and the rowCount is < fetchSize; then i don't need
                    // to go back to the server to know if i'm done.
                    //
                    // for example rowCount = 10; fetchSize = 100; then no need to look for another page
                    //
                    return endOfData();

                } else if (index == fetchSize) {

                    // the page has no more results, but rowCount = fetchSize.  need to check server for more results
                    fetchRowIterator = null;

                    index = 0;
                }
            }
        }
    }
}
