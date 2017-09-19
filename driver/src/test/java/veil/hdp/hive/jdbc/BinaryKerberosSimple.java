package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryKerberosSimple extends AbstractConnectionTest {

        /*
        on windows:
            -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini

        must have JCE installed in proper directory <java_home>/jre/lib/security

     */

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin/admin@HDP.LOCAL");
        properties.setProperty("password", "password");
        properties.setProperty("krb5Debug", "true");
        properties.setProperty("jaasDebug", "true");

        String url = "jdbc:hive2://" + host + ":10000/tests?authMode=KERBEROS&krb5Mode=PASSWORD&krb5ServerPrincipal=hive/" + host + "@HDP.LOCAL";

        return new HiveDriver().connect(url, properties);
    }

}