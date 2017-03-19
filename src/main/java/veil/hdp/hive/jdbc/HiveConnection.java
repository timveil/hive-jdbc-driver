package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HiveConnection extends AbstractConnection {

    private static final Logger log = LoggerFactory.getLogger(HiveConnection.class);

    // constructor
    private final Properties properties;

    // private
    private TTransport transport;
    private TCLIService.Client client;
    private TSessionHandle sessionHandle;
    private TProtocolVersion protocolVersion;

    private CloseableHttpClient httpClient = null;

    // public getter & setter
    private boolean closed;

    // todo: what does this value actually do
    private boolean autoCommitEnabled;

    HiveConnection(Properties properties) {
        this.properties = properties;
        closed = false;
    }

    Properties getProperties() {
        return properties;
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

    void connect() throws SQLException {


        try {

            TransportMode transportMode = properties.containsKey(HiveDriverStringProperty.TRANSPORT_MODE.getName())
                    ? TransportMode.valueOf(properties.getProperty(HiveDriverStringProperty.TRANSPORT_MODE.getName()))
                    : TransportMode.valueOf(HiveDriverStringProperty.TRANSPORT_MODE.getDefaultValue());

            if (transportMode.equals(TransportMode.binary)) {
                transport = ThriftUtils.createBinaryTransport(properties, getLoginTimeout());
            } else {

                httpClient = HttpUtils.buildClient(properties);

                transport = ThriftUtils.createHttpTransport(properties, httpClient);
            }


            ThriftUtils.openTransport(transport);

            client = ThriftUtils.createClient(transport);

            TOpenSessionResp tOpenSessionResp = HiveServiceUtils.openSession(properties, client);

            Map<String, String> configuration = tOpenSessionResp.getConfiguration();

            if (log.isDebugEnabled()) {
                log.debug("configuration for session returned by thrift {}", configuration);
            }

            protocolVersion = tOpenSessionResp.getServerProtocolVersion();

            sessionHandle = tOpenSessionResp.getSessionHandle();

            closed = false;

        } catch (TException | SaslException e) {
            throw new SQLException(e.getMessage(), "", e);
        }

    }

    @Override
    public void close() throws SQLException {

        if (log.isDebugEnabled()) {
            log.debug("attempting to close {}", this.getClass().getName());
        }

        if (!isClosed()) {

            HiveServiceUtils.closeSession(client, sessionHandle);
            ThriftUtils.closeTransport(transport);
            HttpUtils.closeClient(httpClient);

            client = null;
            sessionHandle = null;
            transport = null;
            protocolVersion = null;
            httpClient = null;

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

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new HiveDatabaseMetaData(this);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommitEnabled;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommitEnabled = autoCommit;
    }

    //todo
    @Override
    public String getCatalog() throws SQLException {
        // no catalog name in Hive
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new HiveStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new HiveStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /*




    @Override
    public String getResultSetSchema() throws SQLException {
        return super.getResultSetSchema();
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
    public void setCatalog(String catalog) throws SQLException {
        super.setCatalog(catalog);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        super.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return super.getSchema();
    }


    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        super.setTransactionIsolation(level);
    }
    */


    private int getLoginTimeout() {
        long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());

        if (timeOut > Integer.MAX_VALUE) {
            timeOut = Integer.MAX_VALUE;
        }

        return (int) timeOut;
    }

    @Override
    public String toString() {
        return "HiveConnection{" +
                "properties=" + properties +
                ", transport=" + transport +
                ", client=" + client +
                ", sessionHandle=" + sessionHandle +
                ", protocolVersion=" + protocolVersion +
                ", closed=" + closed +
                '}';
    }
}
