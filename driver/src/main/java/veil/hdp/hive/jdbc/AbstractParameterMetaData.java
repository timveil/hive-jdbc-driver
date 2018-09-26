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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

class AbstractParameterMetaData implements ParameterMetaData {
    @Override
    public int getParameterCount() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw HiveDriver.notImplemented(this.getClass());
    }
}
