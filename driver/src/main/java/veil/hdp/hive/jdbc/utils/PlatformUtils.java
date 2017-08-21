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

    private PlatformUtils() {
    }


    public static boolean isWindows() {
        return isWindows;
    }

    public static String getKrb5LoginModuleClassName() {
        String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

        log.debug("krb5 login module [{}]", loginModule);

        return loginModule;
    }

    public static String getOSLoginModuleClassName() {

        String loginModule = isWindows ? "com.sun.security.auth.module.NTLoginModule" : "com.sun.security.auth.module.UnixLoginModule";

        log.debug("os login module [{}]", loginModule);

        return loginModule;
    }

}
