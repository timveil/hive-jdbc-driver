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
