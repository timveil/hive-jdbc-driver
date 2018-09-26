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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

class JaasConfiguration extends Configuration {

    private static final Logger log = LogManager.getLogger(JaasConfiguration.class);

    private final Map<String, AppConfigurationEntry[]> entries = new HashMap<>(1);

    void addAppConfigEntry(String configurationName, String loginModuleClassName, LoginModuleControlFlag controlFlag, Map<String, ?> options) {

        log.debug("config name [{}], classname [{}], control flag [{}], options [{}]", configurationName, loginModuleClassName, controlFlag, options);

        AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleClassName, controlFlag, options);
        entries.put(configurationName, new AppConfigurationEntry[]{entry});
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return entries.get(name);
    }
}
