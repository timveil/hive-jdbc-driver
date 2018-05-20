package veil.hdp.hive.jdbc.data;

import java.sql.SQLException;

public class CharacterColumn extends AbstractColumn<Character> {
    CharacterColumn(Character value) {
        super(value);
    }


    @Override
    public Character asCharacter() {
        return value;
    }

    @Override
    public String asString() {
        if (value != null) {
            return Character.toString(value);
        }

        return null;
    }
}
