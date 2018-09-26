/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class HiveEmptyResultSet extends AbstractResultSet {

    private static final Logger log = LogManager.getLogger(HiveEmptyResultSet.class);

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
        log.trace("attempting to close {}", this.getClass().getName());
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
