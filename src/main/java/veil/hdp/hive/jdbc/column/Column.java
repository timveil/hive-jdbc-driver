package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Created by tveil on 4/4/17.
 */
public interface Column<T> {

    public ColumnDescriptor getDescriptor();

    public T getValue();

    public Boolean asBoolean() throws SQLException;

    public Date asDate() throws SQLException;

    public Timestamp asTimestamp() throws SQLException;

    public BigDecimal asBigDecimal() throws SQLException;

    public Double asDouble() throws SQLException;

    public Float asFloat() throws SQLException;

    public Integer asInt() throws SQLException;

    public Long asLong() throws SQLException;

    public Short asShort() throws SQLException;

    public String asString() throws SQLException;

    public Byte asByte() throws SQLException;

    public byte[] asByteArray() throws SQLException;

    public InputStream asInputStream() throws SQLException;

    public Time asTime() throws SQLException;

    public Object asObject() throws SQLException;

    public Character asCharacter() throws SQLException;

}
