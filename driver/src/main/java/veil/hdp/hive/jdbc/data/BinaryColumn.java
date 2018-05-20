package veil.hdp.hive.jdbc.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;

public class BinaryColumn extends AbstractColumn<ByteBuffer> {
    BinaryColumn(ByteBuffer value) {
        super(value);
    }


    @Override
    public String asString() {
        if (value != null) {
            return new String(asByteArray());
        }

        return null;
    }

    @Override
    public byte[] asByteArray() {
        if (value != null) {
            return value.array();
        }

        return null;
    }

    @Override
    public InputStream asInputStream() {
        if (value != null) {
            return new ByteArrayInputStream(value.array());
        }

        return null;
    }
}
