package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.test.AbstractConnectionTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HttpKerberosSimpleTest extends AbstractConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {

        Properties properties = new Properties();
        properties.setProperty("user", "timve@LAB.LOCAL");
        properties.setProperty("password", "password");

        String url = "jdbc:hive2://" + host + ":10501/tests?transportMode=http&authMode=KERBEROS&krb5Mode=PASSWORD&krb5ServerPrincipal=hive/hdp2.lab.local@LAB.LOCAL";

        return new HiveDriver().connect(url, properties);
    }

}