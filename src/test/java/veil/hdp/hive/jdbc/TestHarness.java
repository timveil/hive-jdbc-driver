package veil.hdp.hive.jdbc;

import org.apache.hive.jdbc.*;
import org.apache.hive.jdbc.HiveStatement;
import org.junit.Test;

import java.sql.*;
import java.util.List;

import static java.lang.Class.forName;
import static java.lang.System.out;
import static java.sql.DriverManager.getConnection;

public class TestHarness {

    @Test
    public void testNewConnection() throws SQLException, ClassNotFoundException {

        forName("veil.hdp.hive.jdbc.HiveDriver");

        Connection connection = getConnection("jdbc:hive2://hive-large.hdp.local:10000/default", "hive", "dummy");

        connection.close();


    }

    @Test
    public void testOldConnection() throws SQLException, ClassNotFoundException {

        forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = getConnection("jdbc:hive2://hive-large.hdp.local:10000/default", "hive", "dummy");

        Statement statement = connection.createStatement();
        boolean execute = statement.execute("select * from test_table");

        ResultSet rs = statement.getResultSet();

        while (rs.next()) {

            out.println(rs.getDouble("col_double"));
        }

        org.apache.hive.jdbc.HiveStatement hiveStatement = (HiveStatement) statement;

        out.println("yarn guid [" + hiveStatement.getYarnATSGuid() + "]");


        statement.close();
        rs.close();
        connection.close();


    }
}
