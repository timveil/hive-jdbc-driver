package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
public class CharacterColumn extends AbstractColumn<Character> {
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
