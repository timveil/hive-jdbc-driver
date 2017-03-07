package veil.hdp.hive.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.List;

/**
 * Created by timve on 3/5/2017.
 */
public class TestHarness {

    @Test
    public void testNewConnection() throws SQLException, ClassNotFoundException {

        Class.forName("veil.hdp.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection("jdbc:hive2://hive-large.hdp.local:10000/default", "hive", "dummy");

        connection.close();


    }

    @Test
    public void testOldConnection() throws SQLException, ClassNotFoundException {

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection("jdbc:hive2://hive-large.hdp.local:10000/default", "hive", "dummy");

        Statement statement = connection.createStatement();
        boolean execute = statement.execute("show databases");

        ResultSet rs = statement.getResultSet();

        while(rs.next()) {
            System.out.println(rs.getString("database_name"));
        }

        org.apache.hive.jdbc.HiveStatement hiveStatement = (org.apache.hive.jdbc.HiveStatement)statement;

        System.out.println("yarn guid [" + hiveStatement.getYarnATSGuid() +"]");


        statement.close();
        rs.close();
        connection.close();


    }
}
