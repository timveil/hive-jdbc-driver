package veil.hdp.hive.jdbc;

import java.io.InputStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class HivePreparedStatement extends AbstractPreparedStatement {


    private HivePreparedStatement(HiveConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void clearParameters() throws SQLException {
        super.clearParameters();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute();
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        super.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        super.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        super.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        super.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        super.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        super.setDouble(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        super.setString(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        super.setDate(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        super.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        super.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        super.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        super.setNull(parameterIndex, sqlType, typeName);
    }
}
