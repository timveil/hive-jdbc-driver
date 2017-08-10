package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class BooleanColumn extends BaseColumn<Boolean> {
    BooleanColumn(ColumnDescriptor descriptor, Boolean value) {
        super(descriptor, value);
    }

    @Override
    public Boolean getValue() {
        return value != null ? value : false;
    }

    @Override
    public Boolean asBoolean() throws SQLException {
        return getValue();
    }


    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return Boolean.toString(value);
        }

        return null;
    }
}
