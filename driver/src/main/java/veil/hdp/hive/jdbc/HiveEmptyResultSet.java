package veil.hdp.hive.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class HiveEmptyResultSet extends AbstractResultSet {

    private static final Logger log = LoggerFactory.getLogger(HiveResultSet.class);

    // constructor
    private final Schema schema;

    private HiveEmptyResultSet(Schema schema) {
        this.schema = schema;
    }

    public static HiveEmptyResultSetBuilder builder() {
        return new HiveEmptyResultSetBuilder();
    }

    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (schema != null) {
            return HiveResultSetMetaData.builder().schema(schema).build();
        }

        return null;
    }

    @Override
    public void close() throws SQLException {

        if (log.isTraceEnabled()) {
            log.trace("attempting to close {}", this.getClass().getName());
        }

        if (schema != null) {
            schema.clear();
        }
    }

    public static class HiveEmptyResultSetBuilder implements Builder<HiveEmptyResultSet> {

        private Schema schema;

        private HiveEmptyResultSetBuilder() {
        }

        public HiveEmptyResultSetBuilder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public HiveEmptyResultSet build() {
            return new HiveEmptyResultSet(schema);
        }
    }
}