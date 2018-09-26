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

package veil.hdp.hive.jdbc.security;

import org.apache.thrift.transport.TTransport;

import java.security.PrivilegedExceptionAction;

class PrivilegedTransportAction implements PrivilegedExceptionAction<Void> {

    private final TTransport transport;

    PrivilegedTransportAction(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public Void run() throws Exception {
        transport.open();

        return null;
    }
}
