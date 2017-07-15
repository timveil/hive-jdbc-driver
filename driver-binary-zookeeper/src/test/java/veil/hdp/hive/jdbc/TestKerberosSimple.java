package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.BaseConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class TestKerberosSimple extends BaseConnectionTest {

        /*
        on windows:
            -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini

        must have JCE installed in proper directory <java_home>/jre/lib/security

     */

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "timve@LAB.LOCAL");
        properties.setProperty("password", "password");
        properties.setProperty("zkNamespace", "hiveserver2-hive2");

        String url = "jdbc:hive2://" + host + ":2181/jdbc_test?authMode=KERBEROS&krb5Mode=PASSWORD";

        return new BinaryZookeeperHiveDriver().connect(url, properties);
    }

}