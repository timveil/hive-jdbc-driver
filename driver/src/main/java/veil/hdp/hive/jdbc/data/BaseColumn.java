package veil.hdp.hive.jdbc.data;

import org.slf4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.HiveDriver;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;
import veil.hdp.hive.jdbc.utils.SqlDateTimeUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.BitSet;

import static org.slf4j.LoggerFactory.getLogger;
import static veil.hdp.hive.jdbc.metadata.HiveType.*;

public class BaseColumn<T> implements Column<T> {

    static final Logger log = getLogger(BaseColumn.class);

    final ColumnDescriptor descriptor;

    final T value;

    BaseColumn(ColumnDescriptor columnDescriptor, T value) {
        this.descriptor = columnDescriptor;
        this.value = value;
    }

    public static BaseColumnBuilder builder() {
        return new BaseColumnBuilder();
    }

    public ColumnDescriptor getDescriptor() {
        return descriptor;
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

        public BaseColumnBuilder columnData(ColumnData columnData) {
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

            boolean isNull = bitSet.get(row);

            if (columnData instanceof BooleanColumnData) {
                BooleanColumnData data = (BooleanColumnData) columnData;

                return new BooleanColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof ByteColumnData) {
                ByteColumnData data = (ByteColumnData) columnData;

                return new ByteColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof ShortColumnData) {
                ShortColumnData data = (ShortColumnData) columnData;

                return new ShortColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof IntegerColumnData) {
                IntegerColumnData data = (IntegerColumnData) columnData;

                return new IntegerColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof LongColumnData) {
                LongColumnData data = (LongColumnData) columnData;

                return new LongColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof BinaryColumnData) {
                BinaryColumnData data = (BinaryColumnData) columnData;

                return new BinaryColumn(descriptor, isNull ? null : data.getValues().get(row));

            } else if (columnData instanceof DoubleColumnData) {

                DoubleColumnData data = (DoubleColumnData) columnData;

                Double value = data.getValues().get(row);

                if (hiveType.equals(FLOAT)) {
                    return new FloatColumn(descriptor, isNull ? null : new Float(value));
                } else {
                    return new DoubleColumn(descriptor, isNull ? null : value);
                }

            } else if (columnData instanceof StringColumnData) {
                //may need to convert; STRING, LIST, MAP, STRUCT, UNIONTYPE, DECIMAL, NULL, CHAR, VARCHAR

                StringColumnData data = (StringColumnData) columnData;

                String value = data.getValues().get(row);

                if (hiveType.equals(DECIMAL)) {
                    return new DecimalColumn(descriptor, isNull ? null : new BigDecimal(value));
                } else if (hiveType.equals(CHAR)) {
                    return new CharacterColumn(descriptor, isNull ? null : value.charAt(0));
                } else if (hiveType.equals(VARCHAR)) {
                    return new VarcharColumn(descriptor, isNull ? null : value);
                } else if (hiveType.equals(TIMESTAMP)) {
                    return new TimestampColumn(descriptor, isNull ? null : SqlDateTimeUtils.convertStringToTimestamp(value));
                } else if (hiveType.equals(DATE)) {
                    return new DateColumn(descriptor, isNull ? null : SqlDateTimeUtils.convertStringToDate(value));
                } else {
                    return new StringColumn(descriptor, isNull ? null : value);
                }

                // todo; add others

            } else {
                throw new IllegalStateException(MessageFormat.format("unknown instance of ColumnData [{0}]", columnData));
            }


        }

    }
}
