package veil.hdp.hive.jdbc.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveDriver;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;
import veil.hdp.hive.jdbc.utils.SqlDateTimeUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.BitSet;

import static veil.hdp.hive.jdbc.metadata.HiveType.*;

public class BaseColumn<T> implements Column<T> {

    static final Logger log =  LogManager.getLogger(BaseColumn.class);

    final T value;

    BaseColumn(T value) {
        this.value = value;
    }

    public static BaseColumnBuilder builder() {
        return new BaseColumnBuilder();
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Boolean asBoolean() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asBoolean");
    }

    @Override
    public Date asDate() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asDate");
    }

    @Override
    public Timestamp asTimestamp() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asTimestamp");
    }

    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asBigDecimal");
    }

    @Override
    public Double asDouble() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asDouble");
    }

    @Override
    public Float asFloat() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asFloat");
    }

    @Override
    public Integer asInt() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asInt");
    }

    @Override
    public Long asLong() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asLong");
    }

    @Override
    public Short asShort() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asShort");
    }

    @Override
    public String asString() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asString");
    }

    @Override
    public Byte asByte() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asByte");
    }

    @Override
    public byte[] asByteArray() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asByteArray");
    }

    @Override
    public InputStream asInputStream() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asInputStream");
    }

    @Override
    public Time asTime() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asTime");
    }

    @Override
    public Character asCharacter() throws SQLException {
        throw HiveDriver.notImplemented(this.getClass(), "asCharacter");
    }

    public static class BaseColumnBuilder implements Builder<Column> {

        private ColumnData columnData;
        private int row;

        private BaseColumnBuilder() {
        }

        BaseColumnBuilder columnData(ColumnData columnData) {
            this.columnData = columnData;
            return this;
        }


        public BaseColumnBuilder row(int row) {
            this.row = row;
            return this;
        }

        public Column build() {

            ColumnDescriptor descriptor = columnData.getDescriptor();

            HiveType hiveType = descriptor.getColumnType().getHiveType();

            BitSet bitSet = columnData.getNulls();

            Object value = columnData.getValue(row);

            boolean isNull = bitSet.get(row);

            if (columnData instanceof BooleanColumnData) {
                return new BooleanColumn(isNull ? null : (Boolean) value);

            } else if (columnData instanceof ByteColumnData) {
                return new ByteColumn(isNull ? null : (Byte) value);

            } else if (columnData instanceof ShortColumnData) {
                return new ShortColumn(isNull ? null : (Short) value);

            } else if (columnData instanceof IntegerColumnData) {
                return new IntegerColumn(isNull ? null : (Integer) value);

            } else if (columnData instanceof LongColumnData) {
                return new LongColumn(isNull ? null : (Long) value);

            } else if (columnData instanceof BinaryColumnData) {
                return new BinaryColumn(isNull ? null : (ByteBuffer) value);

            } else if (columnData instanceof DoubleColumnData) {

                Double aDouble = (Double) value;

                if (hiveType == FLOAT) {
                    return new FloatColumn(isNull ? null : new Float(aDouble));
                } else {
                    return new DoubleColumn(isNull ? null : aDouble);
                }

            } else if (columnData instanceof StringColumnData) {
                //may need to convert; STRING, LIST, MAP, STRUCT, UNIONTYPE, DECIMAL, NULL, CHAR, VARCHAR

                String aString = (String) value;

                if (hiveType == DECIMAL) {
                    return new DecimalColumn(isNull ? null : new BigDecimal(aString));
                } else if (hiveType == CHAR) {
                    return new CharacterColumn(isNull ? null : aString.charAt(0));
                } else if (hiveType == VARCHAR) {
                    return new VarcharColumn(isNull ? null : aString);
                } else if (hiveType == TIMESTAMP) {
                    return new TimestampColumn(isNull ? null : SqlDateTimeUtils.convertStringToTimestamp(aString));
                } else if (hiveType == DATE) {
                    return new DateColumn(isNull ? null : SqlDateTimeUtils.convertStringToDate(aString));
                } else {
                    return new StringColumn(isNull ? null : aString);
                }

                // todo; add others

            } else {
                throw new IllegalStateException(MessageFormat.format("unknown instance of ColumnData [{0}]", columnData));
            }

        }

    }
}
