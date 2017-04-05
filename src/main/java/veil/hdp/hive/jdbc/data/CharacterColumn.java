package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.sql.SQLException;

public class CharacterColumn extends BaseColumn<Character> {
    public CharacterColumn(ColumnDescriptor descriptor, Character value) {
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
