package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class BooleanColumn extends AbstractColumn<Boolean> {
    BooleanColumn(Boolean value) {
        super(value);
    }

    @Override
    public Boolean getValue() {
        return value != null ? value : false;
    }

    @Override
    public Boolean asBoolean() {
        return getValue();
    }


    @Override
    public String asString() {
        if (value != null) {
            return Boolean.toString(value);
        }

        return null;
    }
}
