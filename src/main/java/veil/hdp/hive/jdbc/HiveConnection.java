package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;
import veil.hdp.hive.jdbc.utils.ThriftUtils;

import java.sql.*;

public class HiveConnection extends AbstractConnection {

    private static final Logger log = LoggerFactory.getLogger(HiveConnection.class);

    // constructor
    private final HiveConfiguration hiveConfiguration;
    private final TTransport transport;
    private final TCLIService.Client client;
    private final TSessionHandle sessionHandle;
    private final TProtocolVersion protocolVersion;

    // public getter & setter
    private boolean closed;

    HiveConnection(HiveConfiguration hiveConfiguration, TTransport transport, TCLIService.Client client, TSessionHandle sessionHandle, TProtocolVersion protocolVersion) {
        this.hiveConfiguration = hiveConfiguration;
        this.transport = transport;
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.protocolVersion = protocolVersion;

        closed = false;
    }

    HiveConfiguration getHiveConfiguration() {
        return hiveConfiguration;
    }

    TTransport getTransport() {
        return transport;
    }

    TCLIService.Client getClient() {
        return client;
    }

    TSessionHandle getSessionHandle() {
        return sessionHandle;
    }

    TProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public void close() throws SQLException {

        if (log.isDebugEnabled()) {
            log.debug("attempting to close {}", this.getClass().getName());
        }

        if (!isClosed()) {

            HiveServiceUtils.closeSession(client, sessionHandle);
            ThriftUtils.closeTransport(transport);

            closed = true;
        }

    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new HiveStatement(this);
    }


    /*

    @Override
    public boolean getAutoCommit() throws SQLException {
        return super.getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return super.getCatalog();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return super.getMetaData();
    }

    @Override
    public String getSchema() throws SQLException {
        return super.getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return super.getTransactionIsolation();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return super.isReadOnly();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return super.prepareStatement(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        super.setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        super.setCatalog(catalog);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        super.setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        super.setTransactionIsolation(level);
    }
    */

    @Override
    public String toString() {
        return "HiveConnection{" +
                "hiveConfiguration=" + hiveConfiguration +
                ", transport=" + transport +
                ", client=" + client +
                ", sessionHandle=" + sessionHandle +
                ", protocolVersion=" + protocolVersion +
                ", closed=" + closed +
                '}';
    }
}
