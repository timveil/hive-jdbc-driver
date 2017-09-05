package veil.hdp.hive.jdbc.data;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public interface Column<T> {

    T getValue();

    Boolean asBoolean() throws SQLException;

    Date asDate() throws SQLException;

    Timestamp asTimestamp() throws SQLException;

    BigDecimal asBigDecimal() throws SQLException;

    Double asDouble() throws SQLException;

    Float asFloat() throws SQLException;

    Integer asInt() throws SQLException;

    Long asLong() throws SQLException;

    Short asShort() throws SQLException;

    String asString() throws SQLException;

    Byte asByte() throws SQLException;

    byte[] asByteArray() throws SQLException;

    InputStream asInputStream() throws SQLException;

    Time asTime() throws SQLException;

    Character asCharacter() throws SQLException;

}
