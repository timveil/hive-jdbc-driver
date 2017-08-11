package veil.hdp.hive.jdbc;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


public class OriginalHiveDriverHttpKerberosConnectionTest extends AbstractConnectionTest {

    @Override
    public Connection createConnection(String host) throws SQLException {

        Properties properties = new Properties();
        properties.setProperty("user", "hive");

        String url = "jdbc:hive2://" + host + ":10501/jdbc_test;principal=hive/hdp2.lab.local@LAB.LOCAL;transportMode=http;httpPath=cliservice";

        return new HiveDriver().connect(url, properties);
    }
}
