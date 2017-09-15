package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryKerberosOSTest extends AbstractConnectionTest {

        /*
        on windows:
            -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini

        must have JCE installed in proper directory <java_home>/jre/lib/security

     */

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();

        String url = "jdbc:hive2://" + host + ":10000/tests?authMode=KERBEROS&krb5Mode=OS&krb5ServerPrincipal=hive/hdp2.lab.local@LAB.LOCAL";


        return new HiveDriver().connect(url, properties);


    }

}