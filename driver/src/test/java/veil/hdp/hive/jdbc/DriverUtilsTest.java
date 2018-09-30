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


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import veil.hdp.hive.jdbc.test.BaseTest;
import veil.hdp.hive.jdbc.utils.DriverUtils;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class DriverUtilsTest extends BaseTest {


    private Properties suppliedProperties;
    private String url;


    @BeforeEach
    public void setUp() throws Exception {

        url = "jdbc:hive2://somehost:10000/test?transportMode=http&whoknows=me";

        suppliedProperties = new Properties();
        suppliedProperties.setProperty("user", "hive");
    }

    @AfterEach
    public void tearDown() throws Exception {

        url = null;
        suppliedProperties = null;
    }

    @Test
    public void acceptURL() throws SQLException {


        try {
            // expect failure
            DriverUtils.acceptURL(null);
        } catch (SQLException e) {
            log.warn("expected error because of null url", e);
        }

        // expect success
        boolean accepted = DriverUtils.acceptURL(url);

        log.debug("accepted: {}", url);
    }

    @Test
    public void buildDriverPropertyInfo() throws Exception {


        DriverPropertyInfo[] driverPropertyInfos = DriverUtils.buildDriverPropertyInfo(url, suppliedProperties);

        for (DriverPropertyInfo info : driverPropertyInfos) {
            log.debug("info.name [{}], info.value [{}]", info.name, info.value);
        }

    }

    @Test
    public void buildProperties() throws Exception {


        Properties newProperties = DriverUtils.buildProperties(url, suppliedProperties);

        log.debug(newProperties.toString());

    }


}