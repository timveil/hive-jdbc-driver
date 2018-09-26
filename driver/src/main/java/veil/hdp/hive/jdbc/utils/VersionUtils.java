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

public final class VersionUtils {

    public static final String HIVE_VERSION = PropertyUtils.getInstance().getValue("hive.version", "0.0");
    public static final String DRIVER_VERSION = PropertyUtils.getInstance().getValue("driver.version", "0.0");

    private VersionUtils() {
    }


    public static int getDriverMajorVersion() {
        return getVersionAtIndex(DRIVER_VERSION, 0);
    }

    public static int getDriverMinorVersion() {
        return getVersionAtIndex(DRIVER_VERSION, 1);
    }

    public static int getHiveMajorVersion() {
        return getVersionAtIndex(HIVE_VERSION, 0);
    }


    public static int getHiveMinorVersion() {
        return getVersionAtIndex(HIVE_VERSION, 1);
    }

    private static int getVersionAtIndex(String version, int index) {
        String[] parts = version.split("\\.");
        return Integer.parseInt(parts[index]);
    }
}
