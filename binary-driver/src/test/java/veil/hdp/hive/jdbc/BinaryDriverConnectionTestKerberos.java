package veil.hdp.hive.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryDriverConnectionTestKerberos extends BaseConnectionTest {

        /*
        -Dsun.security.krb5.debug=true
        -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini
        -Djavax.security.auth.useSubjectCredsOnly=false

        must be kinited first locally

        must have JCE installed in proper directory <java_home>/jre/lib/security

     */

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10500/jdbc_test?authMode=KERBEROS&principal=hive/hdp2.lab.local@LAB.LOCAL";

        return new BinaryHiveDriver().connect(url, properties);
    }

}