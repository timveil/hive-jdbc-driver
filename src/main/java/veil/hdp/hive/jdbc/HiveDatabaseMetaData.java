package veil.hdp.hive.jdbc;

import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.thrift.TGetInfoReq;
import org.apache.hive.service.cli.thrift.TGetInfoResp;
import org.apache.hive.service.cli.thrift.TGetInfoType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import veil.hdp.hive.jdbc.utils.HiveServiceUtils;

import java.sql.SQLException;

public class HiveDatabaseMetaData extends AbstractDatabaseMetaData {

    private static final Logger log = LoggerFactory.getLogger(HiveDatabaseMetaData.class);

    private final HiveConnection connection;

    public HiveDatabaseMetaData(HiveConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        String serverInfoAsString = getServerInfoAsString(GetInfoType.CLI_DATA_SOURCE_READ_ONLY.toTGetInfoType());

        return false;
    }

    private String getServerInfoAsString(TGetInfoType type) throws SQLException {
        String info = null;
        try {
            TGetInfoResp serverInfo = HiveServiceUtils.getServerInfo(this.connection.getClient(), this.connection.getSessionHandle(), type);

            info = serverInfo.getInfoValue().getStringValue();
        } catch (TException e) {
            throw new SQLException(e.getMessage(), "", e);
        }
        return info;

    }

}
