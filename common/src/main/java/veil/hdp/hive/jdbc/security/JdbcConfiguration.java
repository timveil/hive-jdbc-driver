package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

public class JdbcConfiguration extends Configuration {

    public static final String KERBEROS_OS = "kerberos-os";
    public static final String KERBEROS_TICKET_CACHE = "kerberos-ticket-cache";
    public static final String KERBEROS_KEYTAB = "kerberos-keytab";

    private static final Logger log = LoggerFactory.getLogger(JdbcConfiguration.class);


    private Properties properties;

    public JdbcConfiguration(Properties properties) {
        this.properties = properties;
    }

    private static AppConfigurationEntry buildKeytabEntry(Properties properties) {
        Map<String, String> options = new HashMap<String, String>(7);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));
        options.put(LoginModuleConstants.DO_NOT_PROMPT, "true");
        options.put(LoginModuleConstants.KEY_TAB, HiveDriverProperty.KERBEROS_LOCAL_KEYTAB.get(properties));
        options.put(LoginModuleConstants.PRINCIPAL, HiveDriverProperty.KERBEROS_LOCAL_PRINCIPAL.get(properties));
        options.put(LoginModuleConstants.USE_KEY_TAB, "true");
        options.put(LoginModuleConstants.STORE_KEY, "true");
        options.put(LoginModuleConstants.REFRESH_KRB_5_CONFIG, "true");

        return new AppConfigurationEntry(PlatformUtils.getKrb5LoginModuleName(), REQUIRED, options);
    }

/*
    private static AppConfigurationEntry buildJdbcEntry(Properties properties) {
        Map<String, String> options = new HashMap<String, String>(1);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));

        return new AppConfigurationEntry(JdbcLoginModule.class.getName(), REQUIRED, options);
    }
*/

    private static AppConfigurationEntry buildOSEntry(Properties properties) {
        Map<String, String> options = new HashMap<String, String>(1);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));

        return new AppConfigurationEntry(PlatformUtils.getOSLoginModuleName(), REQUIRED, options);
    }

    private static AppConfigurationEntry buildCacheEntry(Properties properties) {
        Map<String, String> options = new HashMap<String, String>(4);
        options.put(LoginModuleConstants.DEBUG, HiveDriverProperty.JAAS_DEBUG_ENABLED.get(properties));
        options.put(LoginModuleConstants.DO_NOT_PROMPT, "true");
        options.put(LoginModuleConstants.USE_TICKET_CACHE, "true");
        options.put(LoginModuleConstants.RENEW_TGT, "true");

        String ticketCache = System.getenv("KRB5CCNAME");

        if (ticketCache != null) {
            options.put(LoginModuleConstants.TICKET_CACHE, ticketCache);
        }


        return new AppConfigurationEntry(PlatformUtils.getKrb5LoginModuleName(), OPTIONAL, options);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {

        if (KERBEROS_OS.equals(name)) {
            //return new AppConfigurationEntry[]{buildOSEntry(properties), buildJdbcEntry(properties)};
            return new AppConfigurationEntry[]{buildOSEntry(properties)};
        } else if (KERBEROS_TICKET_CACHE.equals(name)) {
            //return new AppConfigurationEntry[]{buildOSEntry(properties), buildCacheEntry(properties), buildJdbcEntry(properties)};
            return new AppConfigurationEntry[]{buildCacheEntry(properties)};
        } else if (KERBEROS_KEYTAB.equals(name)) {
            //return new AppConfigurationEntry[]{buildKeytabEntry(properties), buildJdbcEntry(properties)};
            return new AppConfigurationEntry[]{buildKeytabEntry(properties)};
        }

        return null;
    }

    private class LoginModuleConstants {
        public static final String DEBUG = "debug";
        public static final String DO_NOT_PROMPT = "doNotPrompt";
        public static final String KEY_TAB = "keyTab";
        public static final String PRINCIPAL = "principal";
        public static final String USE_KEY_TAB = "useKeyTab";
        public static final String STORE_KEY = "storeKey";
        public static final String REFRESH_KRB_5_CONFIG = "refreshKrb5Config";
        public static final String USE_TICKET_CACHE = "useTicketCache";
        public static final String RENEW_TGT = "renewTGT";
        public static final String TICKET_CACHE = "ticketCache";
    }


}
