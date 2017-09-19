package veil.hdp.hive.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class PlatformUtils {

    private static final Logger log =  LogManager.getLogger(PlatformUtils.class);

    private static final String OS_NAME = System.getProperty("os.name");
    private static final String OS_ARCH = System.getProperty("os.arch");
    private static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

    private static final boolean isWindows = StringUtils.startsWithIgnoreCase(OS_NAME, "windows");

    private static final String KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String NT_LOGIN_MODULE = "com.sun.security.auth.module.NTLoginModule";
    private static final String UNIX_LOGIN_MODULE = "com.sun.security.auth.module.UnixLoginModule";

    private PlatformUtils() {
    }


    public static boolean isWindows() {
        return isWindows;
    }

    public static String getKrb5LoginModuleClassName() {
        String loginModule = KRB5_LOGIN_MODULE;

            log.debug("krb5 login module [{}]", loginModule);

        return loginModule;
    }

    public static String getOSLoginModuleClassName() {

        String loginModule = isWindows ? NT_LOGIN_MODULE : UNIX_LOGIN_MODULE;

            log.debug("os login module [{}]", loginModule);

        return loginModule;
    }

}
