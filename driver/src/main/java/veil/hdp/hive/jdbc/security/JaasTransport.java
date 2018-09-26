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
import org.apache.thrift.transport.TTransportException;
import veil.hdp.hive.jdbc.HiveException;
import veil.hdp.hive.jdbc.thrift.WrappedTransport;

import javax.security.auth.Subject;
import java.security.PrivilegedActionException;

public class JaasTransport extends WrappedTransport {

    private final Subject subject;

    public JaasTransport(TTransport wrapped, Subject subject) {
        super(wrapped);
        this.subject = subject;
    }

    @Override
    public void open() {

        try {
            Subject.doAs(subject, new PrivilegedTransportAction(wrapped));
        } catch (PrivilegedActionException e) {
            throw new HiveException(e);
        }
    }
}
