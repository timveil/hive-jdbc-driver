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

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

class AbstractDataSource implements DataSource {
    @Override
    public final <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final PrintWriter getLogWriter() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final void setLogWriter(PrintWriter out) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public final Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
