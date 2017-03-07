package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.SQLException;
import java.util.Iterator;

public class HiveQueryResultSet extends HiveResultSet {

    private int maxRows;
    private boolean emptyResultSet;
    private int rowsFetched;
    private boolean fetchFirst = false;
    private RowSet fetchedRows;
    private Iterator<Object[]> fetchedRowsItr;
    private int fetchSize;

    private TOperationHandle stmtHandle;
    private TProtocolVersion protocol;
    private TCLIService.Client client;

    private boolean isClosed = false;

    @Override
    public boolean next() throws SQLException {

        if (emptyResultSet || (maxRows > 0 && rowsFetched >= maxRows)) {
            return false;
        }

        try {
            TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT;

            if (fetchFirst) {
                orientation = TFetchOrientation.FETCH_FIRST;
                fetchedRows = null;
                fetchedRowsItr = null;
                fetchFirst = false;
            }

            if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
                TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, orientation, fetchSize);
                TFetchResultsResp fetchResp = client.FetchResults(fetchReq);

                HiveServiceUtils.verifySuccessWithInfo(fetchResp.getStatus());

                TRowSet results = fetchResp.getResults();
                fetchedRows = RowSetFactory.create(results, protocol);
                fetchedRowsItr = fetchedRows.iterator();
            }

            if (fetchedRowsItr.hasNext()) {
                row = fetchedRowsItr.next();
            } else {
                return false;
            }

            rowsFetched++;


        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }

        return true;
    }

    @Override
    public void close() throws SQLException {
        client = null;
        stmtHandle = null;
    }
}
