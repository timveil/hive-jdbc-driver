package veil.hdp.hive.jdbc.utils;

public class VersionUtils {

    public static String HIVE_VERSION = PropertyUtils.getInstance().getValue("hive.version", "0.0");
    public static String DRIVER_VERSION = PropertyUtils.getInstance().getValue("driver.version", "0.0");


    public static int getDriverMajorVersion() {
        return getVersionAtIndex(DRIVER_VERSION, 0);
    }

    public static int getDriverMinorVersion() {
        return getVersionAtIndex(DRIVER_VERSION, 1);
    }

    public static int getHiveMajorVersion() {
        return getVersionAtIndex(HIVE_VERSION, 0);
    }


    public static int getHiveMinorVersion() {
        return getVersionAtIndex(HIVE_VERSION, 1);
    }

    private static int getVersionAtIndex(String version, int index) {
        String[] parts = version.split("\\.");
        return Integer.parseInt(parts[index]);
    }
}
