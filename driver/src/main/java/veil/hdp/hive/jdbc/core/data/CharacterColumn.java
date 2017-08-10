package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class CharacterColumn extends BaseColumn<Character> {
    CharacterColumn(ColumnDescriptor descriptor, Character value) {
        super(descriptor, value);
    }


    @Override
    public Character asCharacter() throws SQLException {
        return getValue();
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return Character.toString(value);
        }

        return null;
    }
}
