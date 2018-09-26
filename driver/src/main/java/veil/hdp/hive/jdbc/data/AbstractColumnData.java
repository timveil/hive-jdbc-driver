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

package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.util.BitSet;
import java.util.List;

public abstract class AbstractColumnData<T> implements ColumnData {

    private final ColumnDescriptor descriptor;
    private final List<T> values;
    private final BitSet nulls;
    private final int rowCount;

    AbstractColumnData(ColumnDescriptor descriptor, List<T> values, BitSet nulls, int rowCount) {
        this.descriptor = descriptor;
        this.values = values;
        this.nulls = nulls;
        this.rowCount = rowCount;
    }

    ColumnDescriptor getDescriptor() {
        return descriptor;
    }

    T getValue(int row) {
        if (isNull(row)) {
            return null;
        }

        return values.get(row);
    }

    private boolean isNull(int row) {
        return nulls.get(row);
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

}
