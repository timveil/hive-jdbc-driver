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

package veil.hdp.hive.jdbc.thrift;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import veil.hdp.hive.jdbc.bindings.TGetOperationStatusResp;
import veil.hdp.hive.jdbc.bindings.TStatus;
import veil.hdp.hive.jdbc.utils.HiveExceptionUtils;


public class HiveThriftException extends RuntimeException {

    private static final Logger log = LogManager.getLogger(HiveThriftException.class);
    private static final long serialVersionUID = 1700514420277606047L;


    public HiveThriftException(TException cause) {
        super(cause);
    }


    public HiveThriftException(TGetOperationStatusResp operationStatusResp) {
        super();

        // todo - need to understand whats available here
        log.warn("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + operationStatusResp);
    }

    public HiveThriftException(TStatus status) {

        super(status.getErrorMessage());

        if (status.getInfoMessages() != null) {
            initCause(HiveExceptionUtils.toStackTrace(status.getInfoMessages()));
        }

    }

}
