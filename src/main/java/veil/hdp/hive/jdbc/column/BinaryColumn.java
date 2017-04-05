package veil.hdp.hive.jdbc.column;

import veil.hdp.hive.jdbc.ColumnDescriptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Created by tveil on 4/4/17.
 */
public class BinaryColumn extends AbstractColumn<ByteBuffer> {
    public BinaryColumn(ColumnDescriptor descriptor, ByteBuffer value) {
        super(descriptor, value);
    }


    @Override
    public String asString() throws SQLException {
        if (value != null) {
            return new String(asByteArray());
        }

        return null;
    }

    @Override
    public byte[] asByteArray() throws SQLException {
        if (value != null) {
            return value.array();
        }

        return null;
    }

    @Override
    public InputStream asInputStream() throws SQLException {
        if (value != null) {
            return new ByteArrayInputStream(value.array());
        }

        return null;
    }
}
