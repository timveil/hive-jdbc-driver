package veil.hdp.hive.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class HiveEmptyResultSet extends AbstractResultSet {

    private final Schema schema;

    private HiveEmptyResultSet(Schema schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new HiveResultSetMetaData.Builder().schema(schema).build();
    }

    @Override
    public void close() throws SQLException {
        schema.clear();
    }

    public static class Builder {

        private Schema schema;


        public HiveEmptyResultSet.Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public HiveEmptyResultSet build() throws SQLException {


            return new HiveEmptyResultSet(schema);
        }
    }
}
