package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class VarcharColumn extends AbstractColumn<String> {
    public VarcharColumn(ColumnDescriptor descriptor, String value) {
        super(descriptor, value);
    }

    @Override
    public String asString() throws SQLException {
        return getValue();
    }

    @Override
    public byte[] asByteArray() throws SQLException {
        if (value != null) {
            return value.getBytes();
        }

        return null;
    }
}