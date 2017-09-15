package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class CharacterColumn extends AbstractColumn<Character> {
    CharacterColumn(Character value) {
        super(value);
    }


    @Override
    public Character asCharacter() throws SQLException {
        return value;
    }

    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return Character.toString(value);
        }

        return null;
    }
}
