package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcConfiguration extends Configuration {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final String SIMPLE_CONFIG_NAME = "hadoop-simple";

    private Properties properties;

    public JdbcConfiguration(Properties properties) {
        this.properties = properties;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {


        if (SIMPLE_CONFIG_NAME.equals(name)) {

            Map<String, String> options = new HashMap<String, String>(1);
            options.put("debug", HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));

            AppConfigurationEntry osConfiguration = new AppConfigurationEntry(PlatformUtils.getOSLoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
            AppConfigurationEntry hadoopConfiguration = new AppConfigurationEntry(JdbcLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);

            return new AppConfigurationEntry[]{osConfiguration, hadoopConfiguration};

        }

        return null;
    }


}
