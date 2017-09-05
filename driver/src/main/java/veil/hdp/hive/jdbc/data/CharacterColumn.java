package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class CharacterColumn extends BaseColumn<Character> {
    CharacterColumn(Character value) {
        super(value);
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
