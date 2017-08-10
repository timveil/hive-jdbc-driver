package veil.hdp.hive.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlatformUtils {

    private static final Logger log = LoggerFactory.getLogger(PlatformUtils.class);

    private static final String OS_NAME = System.getProperty("os.name");
    private static final String OS_ARCH = System.getProperty("os.arch");
    private static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

    private static final boolean isWindows = StringUtils.startsWithIgnoreCase(OS_NAME, "windows");


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
