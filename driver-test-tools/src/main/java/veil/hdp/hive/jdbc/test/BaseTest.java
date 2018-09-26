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

package veil.hdp.hive.jdbc.test;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class BaseTest {

    protected static final Logger log = LogManager.getLogger(BaseTest.class);

    public BaseTest() {
        java.util.logging.Logger.getLogger("javax.security.sasl").setLevel(java.util.logging.Level.FINEST);
    }

    public static String getHost() {
        String host = System.getProperty("test.host", "jdbc-binary.hdp.local");

        if (host == null) {
            throw new IllegalArgumentException("can't run test without a valid host.  You must set the property \"test.host\" before executing test.  For example: -Dtest.host=jdbc-binary.hdp.local");
        }

        return host;
    }

    public static int getTestRuns() {
        return Integer.parseInt(System.getProperty("test.runs", "10"));
    }

}


