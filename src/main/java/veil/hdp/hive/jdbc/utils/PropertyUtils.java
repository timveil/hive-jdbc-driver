package veil.hdp.hive.jdbc.utils;


import veil.hdp.hive.jdbc.ConnectionParameters;

import java.util.Map;
import java.util.Properties;

public class PropertyUtils {

    private static final String HIVE_VAR_PREFIX = "hivevar:";
    private static final String HIVE_CONF_PREFIX = "hiveconf:";
    private static final String AUTH_TYPE = "auth";
    private static final String AUTH_USER = "user";
    private static final String AUTH_PASSWD = "password";


    // hack: don't like this.  can we rethink this and put it somewhere else
    public static void mergeProperties(ConnectionParameters connParams, Properties info) {
        for (Map.Entry<Object, Object> kv : info.entrySet()) {
            if ((kv.getKey() instanceof String)) {
                String key = (String) kv.getKey();
                if (key.startsWith(HIVE_VAR_PREFIX)) {
                    connParams.getHiveVariables().put(key.substring(HIVE_VAR_PREFIX.length()), info.getProperty(key));
                } else if (key.startsWith(HIVE_CONF_PREFIX)) {
                    connParams.getHiveConfigurationParameters().put(key.substring(HIVE_CONF_PREFIX.length()), info.getProperty(key));
                }
            }
        }

        if (info.containsKey(AUTH_USER)) {
            connParams.getSessionVariables().put(AUTH_USER, info.getProperty(AUTH_USER));

        }
        if (info.containsKey(AUTH_PASSWD)) {
            connParams.getSessionVariables().put(AUTH_PASSWD, info.getProperty(AUTH_PASSWD));
        }

        if (info.containsKey(AUTH_TYPE)) {
            connParams.getSessionVariables().put(AUTH_TYPE, info.getProperty(AUTH_TYPE));
        }

    }

}
