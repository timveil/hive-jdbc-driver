package veil.hdp.hive.jdbc.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlatformUtils {

    private static final Logger log = LoggerFactory.getLogger(PlatformUtils.class);

    private static final String OS_NAME = System.getProperty("os.name");
    private static final String OS_ARCH = System.getProperty("os.arch");
    private static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

    private static final boolean isIBM = JAVA_VENDOR_NAME.contains("IBM");
    private static final boolean isWindows = OS_NAME.startsWith("Windows");
    private static final boolean is64Bit = OS_ARCH.contains("64");
    private static final boolean isAIX = OS_NAME.equals("AIX");

    public static String getKrb5LoginModuleName() {
        String loginModule =  isIBM ? "com.ibm.security.auth.module.Krb5LoginModule" : "com.sun.security.auth.module.Krb5LoginModule";

        log.debug("krb5 login module [{}]", loginModule);

        return loginModule;
    }

    public static String getOSLoginModuleName() {

        String loginModule;

        if (isIBM) {
            if (isWindows) {
                loginModule = is64Bit ? "com.ibm.security.auth.module.Win64LoginModule"
                        : "com.ibm.security.auth.module.NTLoginModule";
            } else if (isAIX) {
                loginModule = is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule"
                        : "com.ibm.security.auth.module.AIXLoginModule";
            } else {
                loginModule = "com.ibm.security.auth.module.LinuxLoginModule";
            }
        } else {
            loginModule = isWindows ? "com.sun.security.auth.module.NTLoginModule"
                    : "com.sun.security.auth.module.UnixLoginModule";
        }

        log.debug("os login module [{}]", loginModule);

        return loginModule;
    }

    public static Class<? extends java.security.Principal> getOsPrincipalClass() {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        try {
            String principalClass;

            if (isIBM) {
                if (is64Bit) {
                    principalClass = "com.ibm.security.auth.UsernamePrincipal";
                } else {
                    if (isWindows) {
                        principalClass = "com.ibm.security.auth.NTUserPrincipal";
                    } else if (isAIX) {
                        principalClass = "com.ibm.security.auth.AIXPrincipal";
                    } else {
                        principalClass = "com.ibm.security.auth.LinuxPrincipal";
                    }
                }
            } else {
                principalClass = isWindows ? "com.sun.security.auth.NTUserPrincipal" : "com.sun.security.auth.UnixPrincipal";
            }

            log.debug("principal class [{}]", principalClass);

            return (Class<? extends java.security.Principal>) classLoader.loadClass(principalClass);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
