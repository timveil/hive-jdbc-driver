package veil.hdp.hive.jdbc;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


public class OriginalHiveDriverConnectionTestKerberos extends BaseConnectionTest {

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

        String url = "jdbc:hive2://" + host + ":10500/jdbc_test;principal=hive/hdp2.lab.local@LAB.LOCAL";

        return new HiveDriver().connect(url, properties);
    }
}
