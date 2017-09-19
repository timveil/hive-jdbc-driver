package veil.hdp.hive.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.utils.Constants;
import veil.hdp.hive.jdbc.utils.DriverUtils;
import veil.hdp.hive.jdbc.utils.PropertyUtils;
import veil.hdp.hive.jdbc.utils.VersionUtils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;

public class HiveDriver implements Driver {

    private static final Logger log =  LogManager.getLogger(HiveDriver.class);
    private static final String NOT_PROVIDED = "NOT PROVIDED";
    private static final String UNKNOWN = "UNKNOWN";

    static {
        try {

            DriverManager.registerDriver(new HiveDriver());

            if (log.isInfoEnabled()) {
                log.info("driver [{}] has been registered.", HiveDriver.class.getName());
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName, String reason) {
        return new SQLFeatureNotSupportedException(MessageFormat.format("Method [{0}] is not implemented. Reason: [{1}]", callClass.getName() + '.' + functionName, reason));
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass, String functionName) {
        return notImplemented(callClass, functionName, NOT_PROVIDED);
    }

    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass) {
        return notImplemented(callClass, UNKNOWN);
    }

    private static Connection connect(Properties properties) {

        PropertyUtils.printProperties(properties);

        System.setProperty(Constants.SUN_SECURITY_KRB5_DEBUG, HiveDriverProperty.KERBEROS_DEBUG_ENABLED.get(properties));
        System.setProperty(Constants.JAVAX_SECURITY_AUTH_USE_SUBJECT_CREDS_ONLY, HiveDriverProperty.KERBEROS_USE_SUBJECT_CREDENTIALS_ONLY.get(properties));

        return HiveConnection.builder().properties(properties).build();
    }


    public Connection connect(String url, Properties info) throws SQLException {

        if (log.isInfoEnabled()) {
            log.info("connecting to hive version [{}] with thrift protocol version [{}].  Please ensure these match your HiveServer2 instance or unexpected errors and behavior may occur.",
                    PropertyUtils.getInstance().getValue("hive.version"),
                    PropertyUtils.getInstance().getValue("thrift.protocol.version.default"));
        }

        url = StringUtils.trimToNull(url);

        if (acceptsURL(url)) {
            Properties properties = DriverUtils.buildProperties(url, info);
            return connect(properties);
        }

        return null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DriverUtils.buildDriverPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return VersionUtils.getDriverMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return VersionUtils.getDriverMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return Boolean.FALSE;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw notImplemented(this.getClass(), "getParentLogger", "This driver uses Log4J2 instead of JUL directly for performance reasons");
    }

    public boolean acceptsURL(String url) throws SQLException {
        return DriverUtils.acceptURL(url);
    }


}
