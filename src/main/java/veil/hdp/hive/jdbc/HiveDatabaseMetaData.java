package veil.hdp.hive.jdbc;

import org.apache.hive.common.HiveVersionAnnotation;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.thrift.TGetInfoResp;
import org.apache.hive.service.cli.thrift.TGetInfoType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveDatabaseMetaData extends AbstractDatabaseMetaData {

    private static final Logger log = LoggerFactory.getLogger(HiveDatabaseMetaData.class);

    private final HiveConnection connection;

    public HiveDatabaseMetaData(HiveConnection connection) {
        this.connection = connection;
    }


    @Override
    public String getDriverName() throws SQLException {
        return HiveDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return new HiveDriver().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return new HiveDriver().getMinorVersion();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Apache Hive";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return HiveVersionInfo.getVersion();
    }

    /**
     * @see HiveDatabaseMetaData getDatabaseProductVersion
     * @return
     * @throws SQLException
     */
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    /**
     * @see HiveDatabaseMetaData getDatabaseProductVersion
     * @return
     * @throws SQLException
     */
    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        String serverInfoAsString = getServerInfoAsString(GetInfoType.CLI_TXN_CAPABLE.toTGetInfoType());
        return serverInfoAsString != null && Boolean.parseBoolean(serverInfoAsString);
    }


    // todo
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    // todo
    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    // todo
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    // todo
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    // todo
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return null;
    }

    // todo
    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    // todo
    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    private String getServerInfoAsString(TGetInfoType type) throws SQLException {
        String info = null;
        try {
            TGetInfoResp serverInfo = HiveServiceUtils.getServerInfo(this.connection.getClient(), this.connection.getSessionHandle(), type);

            info = serverInfo.getInfoValue().getStringValue();
        } catch (TException e) {
            log.debug(e.getMessage(), e);
        }
        return info;

    }

}
