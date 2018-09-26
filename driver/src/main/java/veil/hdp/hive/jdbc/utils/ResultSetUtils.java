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

package veil.hdp.hive.jdbc.utils;

import veil.hdp.hive.jdbc.HiveSQLException;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.sql.SQLException;
import java.text.MessageFormat;

public final class ResultSetUtils {

    private ResultSetUtils() {
    }

    public static int findColumnIndex(Schema schema, String columnLabel) throws SQLException {
        ColumnDescriptor columnDescriptor = schema.getColumn(columnLabel);

        if (columnDescriptor != null) {
            return columnDescriptor.getPosition();
        }

        throw new HiveSQLException(MessageFormat.format("Could not find column for name {0} in Schema {1}", columnLabel, schema));
    }

}
