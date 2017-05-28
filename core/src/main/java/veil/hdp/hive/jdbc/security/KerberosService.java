package veil.hdp.hive.jdbc.security;

import org.apache.commons.lang3.StringUtils;
import org.ietf.jgss.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.HiveDriverProperty;
import veil.hdp.hive.jdbc.HiveException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

public class KerberosService {

    private static final Logger log = LoggerFactory.getLogger(KerberosService.class);


    private static final String KRB5_OID = "1.2.840.113554.1.2.2";
    private static final String KRB5_NAME_OID = "1.2.840.113554.1.2.2.1";


    private static Oid MECHANISM = null;
    private static Oid NAME_TYPE = null;

    static {
        try {
            MECHANISM = new Oid(KRB5_OID);
            NAME_TYPE = new Oid(KRB5_NAME_OID);
        } catch (GSSException e) {
            e.printStackTrace();
        }
    }


    public static Subject getPreAuthenticatedSubject() {
        AccessControlContext preAuthContext = AccessController.getContext();
        Subject subject = Subject.getSubject(preAuthContext);

        log.debug("pre-auth subject [{}]", subject);

        return subject;
    }

    public static byte[] getToken(String serverPrincipal) {

        GSSContext context = null;

        try {

            GSSManager manager = GSSManager.getInstance();
            GSSName name = manager.createName(serverPrincipal, NAME_TYPE);

            context = manager.createContext(name, MECHANISM, null, GSSContext.DEFAULT_LIFETIME);
            context.requestMutualAuth(false);

            byte[] inToken = new byte[0];

            return context.initSecContext(inToken, 0, inToken.length);
        } catch (GSSException e) {
            throw new HiveException(e);
        } finally {

            if (context != null) {
                try {
                    context.dispose();
                } catch (GSSException e) {

                }
            }
        }

    }

    public static Subject getSubject(Properties properties) throws LoginException {

        System.setProperty("sun.security.krb5.debug", HiveDriverProperty.KERBEROS_DEBUG_ENABLED.get(properties));
        System.setProperty("javax.security.auth.useSubjectCredsOnly", HiveDriverProperty.KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY.get(properties));

        KerberosMode kerberosMode = KerberosMode.valueOf(HiveDriverProperty.KERBEROS_MODE.get(properties));

        log.debug("kerberos mode [{}]", kerberosMode);

        boolean debugJaas = HiveDriverProperty.JAAS_DEBUG_ENABLED.getBoolean(properties);

        switch (kerberosMode) {
            case KEYTAB:
                String keyTab = HiveDriverProperty.KERBEROS_USER_KEYTAB.get(properties);
                String keyTabPrincipal = HiveDriverProperty.USER.get(properties);

                return loginWithKeytab(keyTab, keyTabPrincipal, debugJaas);
            case PREAUTH:
                return getPreAuthenticatedSubject();
            case PASSWORD:
                String principal = HiveDriverProperty.USER.get(properties);
                String password = HiveDriverProperty.PASSWORD.get(properties);

                return loginWithPassword(principal, password, debugJaas);
        }

        throw new IllegalArgumentException("kerberos mode [" + kerberosMode + "] is not supported!");

    }

    public static Subject loginWithPassword(String principal, String password, boolean debugJaas) throws LoginException {

        String configName = "fromPassword";

        Map<String, String> options = buildOptions(debugJaas, false, false,
                null, false, false,
                false, null, false, null,
                true, false, false,
                false, true);

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, PlatformUtils.getKrb5LoginModuleClassName(), REQUIRED, options);

        LoginContext context = new LoginContext(configName, null, new UsernamePasswordCallbackHandler(principal, password), config);
        context.login();

        Subject subject = context.getSubject();

        log.debug("successfully logged in subject with password [{}]", subject);

        return subject;
    }

    public static Subject loginFromLocal(boolean debugJaas) throws LoginException {

        String configName = "fromLocal";

        Map<String, String> options = buildOptions(debugJaas, false, false,
                null, false, false,
                false, null, false, null,
                true, false, false,
                false, true);

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, PlatformUtils.getOSLoginModuleClassName(), REQUIRED, options);

        LoginContext context = new LoginContext(configName, null, new NoOpCallbackHandler(), config);
        context.login();

        Subject subject = context.getSubject();

        log.debug("successfully logged in subject with local login module [{}]", subject);

        return subject;
    }

    public static Subject loginWithKeytab(String keyTab, String principal, boolean debugJaas) throws LoginException {


        String configName = "fromKeytab";


        Map<String, String> options = buildOptions(debugJaas, true, false,
                null, false, true,
                true, keyTab, true, principal,
                true, false, false,
                false, true);

        JaasConfiguration config = new JaasConfiguration();
        config.addAppConfigEntry(configName, PlatformUtils.getKrb5LoginModuleClassName(), REQUIRED, options);

        LoginContext context = new LoginContext(configName, null, new NoOpCallbackHandler(), config);
        context.login();

        Subject subject = context.getSubject();

        log.debug("successfully logged in subject with keytab [{}]", subject);

        return subject;
    }

    private static Map<String, String> buildOptions(boolean debug, boolean refreshKrb5Config, boolean useTicketCache, String ticketCache,
                                                    boolean renewTGT, boolean doNotPrompt, boolean useKeyTab,
                                                    String keyTab, boolean storeKey, String principal,
                                                    boolean isInitiator, boolean useFirstPass, boolean tryFirstPass,
                                                    boolean storePass, boolean clearPass) {
        Map<String, String> options = new HashMap<>(16);
        options.put(LoginModuleConstants.DEBUG, Boolean.toString(debug));

        if (PlatformUtils.isWindows()) {
            options.put(LoginModuleConstants.DEBUG_NATIVE, Boolean.toString(debug));
        }

        options.put(LoginModuleConstants.REFRESH_KRB_5_CONFIG, Boolean.toString(refreshKrb5Config));
        options.put(LoginModuleConstants.USE_TICKET_CACHE, Boolean.toString(useTicketCache));

        if (StringUtils.isNotBlank(ticketCache)) {
            options.put(LoginModuleConstants.TICKET_CACHE, ticketCache);
        }

        options.put(LoginModuleConstants.RENEW_TGT, Boolean.toString(renewTGT));
        options.put(LoginModuleConstants.DO_NOT_PROMPT, Boolean.toString(doNotPrompt));
        options.put(LoginModuleConstants.USE_KEY_TAB, Boolean.toString(useKeyTab));

        if (StringUtils.isNotBlank(keyTab)) {
            options.put(LoginModuleConstants.KEY_TAB, keyTab);
        }

        options.put(LoginModuleConstants.STORE_KEY, Boolean.toString(storeKey));

        if (StringUtils.isNotBlank(principal)) {
            options.put(LoginModuleConstants.PRINCIPAL, principal);
        }

        options.put(LoginModuleConstants.IS_INITIATOR, Boolean.toString(isInitiator));
        options.put(LoginModuleConstants.USE_FIRST_PASS, Boolean.toString(useFirstPass));
        options.put(LoginModuleConstants.TRY_FIRST_PASS, Boolean.toString(tryFirstPass));
        options.put(LoginModuleConstants.STORE_PASS, Boolean.toString(storePass));
        options.put(LoginModuleConstants.CLEAR_PASS, Boolean.toString(clearPass));

        return options;
    }

    private class LoginModuleConstants {
        public static final String DEBUG = "debug";
        public static final String DEBUG_NATIVE = "debugNative";
        public static final String DO_NOT_PROMPT = "doNotPrompt";
        public static final String KEY_TAB = "keyTab";
        public static final String PRINCIPAL = "principal";
        public static final String USE_KEY_TAB = "useKeyTab";
        public static final String STORE_KEY = "storeKey";
        public static final String REFRESH_KRB_5_CONFIG = "refreshKrb5Config";
        public static final String USE_TICKET_CACHE = "useTicketCache";
        public static final String USE_FIRST_PASS = "useFirstPass";
        public static final String TRY_FIRST_PASS = "tryFirstPass";
        public static final String STORE_PASS = "storePass";
        public static final String CLEAR_PASS = "clearPass";
        public static final String RENEW_TGT = "renewTGT";
        public static final String TICKET_CACHE = "ticketCache";
        public static final String IS_INITIATOR = "isInitiator";
    }
}
