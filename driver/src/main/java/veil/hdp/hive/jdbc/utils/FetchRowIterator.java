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

import com.google.common.collect.AbstractIterator;
import veil.hdp.hive.jdbc.data.ColumnBasedSet;
import veil.hdp.hive.jdbc.data.Row;

class FetchRowIterator extends AbstractIterator<Row> {

    private final ColumnBasedSet columnBasedSet;
    private int index = 0;

    public FetchRowIterator(ColumnBasedSet columnBasedSet) {
        this.columnBasedSet = columnBasedSet;
    }

    @Override
    protected Row computeNext() {

        int rowCount = columnBasedSet.getRowCount();

        if (rowCount <= 0) {
            return endOfData();
        }

        if (index < rowCount) {
            Row row = Row.builder().columnBasedSet(columnBasedSet).row(index).build();

            index++;

            return row;
        }

        index = 0;

        return endOfData();
    }
}
