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

import org.apache.hive.jdbc.HiveDriver;
import veil.hdp.hive.jdbc.test.AbstractDriverTest;

import java.sql.Driver;
import java.sql.SQLException;

public class OriginalDriverTest extends AbstractDriverTest {

    @Override
    public Driver createDriver() throws SQLException {
        return new HiveDriver();
    }
}