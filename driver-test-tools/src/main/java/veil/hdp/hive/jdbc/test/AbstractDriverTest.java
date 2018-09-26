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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractDriverTest extends BaseTest {

    private Driver driver;

    public abstract Driver createDriver() throws SQLException;

    @BeforeEach
    public void setUp() throws Exception {
        driver = createDriver();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (driver != null) {
            driver = null;
        }
    }

    @Test
    public void connect() throws Exception {

        Connection connection = null;
        String url;

        Properties properties = new Properties();
        properties.setProperty("user", "hive");
        properties.setProperty("httpPath", "xxxx");
        properties.setProperty("sslTrustStore", "dummy store");
        properties.setProperty("zooKeeperNamespace", "zzzzzz");

        url = "jdbc:hive2://" + getHost() + ":10000/default";

        try {
            connection = driver.connect(url, properties);

            assertNotNull(connection);
        } finally {

            if (connection != null) {
                connection.close();
            }
        }


    }

    @Test
    public void acceptsURL() throws Exception {
        String url = "jdbc:hive2://foo";

        boolean acceptsURL = driver.acceptsURL(url);

        assertTrue(acceptsURL);

        url = "jdbc:mysql://foo";

        acceptsURL = driver.acceptsURL(url);

        assertFalse(acceptsURL);
    }
}
