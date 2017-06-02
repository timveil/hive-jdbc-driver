package veil.hdp.hive.jdbc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class HiveDataSource extends AbstractDataSource {

    private static final Logger log = LoggerFactory.getLogger(HiveDataSource.class);

    private String serverName;
    private String databaseName;
    private String user;
    private String password;
    private int portNumber;

    @Override
    public Connection getConnection() throws SQLException {
        return super.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return super.getConnection(username, password);
    }


}
