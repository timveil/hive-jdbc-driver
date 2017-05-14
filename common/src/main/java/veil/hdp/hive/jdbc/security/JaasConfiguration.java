package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

public class JaasConfiguration extends Configuration {

    private static final Logger log = LoggerFactory.getLogger(JaasConfiguration.class);

    private final Map<String, AppConfigurationEntry[]> entries = new HashMap<>(1);

    public void addAppConfigEntry(String configurationName, String loginModuleClassName, LoginModuleControlFlag controlFlag, Map<String, ?> options) {

        log.debug("config name [{}], classname [{}], control flag [{}], options [{}]", configurationName, loginModuleClassName, controlFlag, options);

        AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleClassName, controlFlag, options);
        entries.put(configurationName, new AppConfigurationEntry[]{entry});
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return entries.get(name);
    }
}
