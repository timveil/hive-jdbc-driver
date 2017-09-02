package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;

import java.text.MessageFormat;
import java.util.BitSet;
import java.util.List;

public class ColumnData<T> {

    private static final byte[] MASKS = {
            0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80
    };


    private final ColumnDescriptor descriptor;
    private final List<T> values;
    private final BitSet nulls;

    ColumnData(ColumnDescriptor descriptor, List<T> values, BitSet nulls) {
        this.descriptor = descriptor;
        this.values = values;
        this.nulls = nulls;
    }

    public static ColumnDataBuilder builder() {
        return new ColumnDataBuilder();
    }

    public ColumnDescriptor getDescriptor() {
        return descriptor;
    }

    public List<T> getValues() {
        return values;
    }

    public T getValue(int row) {
        return values.get(row);
    }

    public BitSet getNulls() {
        return nulls;
    }

    public int getRowCount() {
        return values.size();
    }

    public static class ColumnDataBuilder implements Builder<ColumnData> {

        private TColumn column;
        private ColumnDescriptor columnDescriptor;

        private ColumnDataBuilder() {
        }

        private static BitSet buildBitSet(byte[] nulls) {
            int nullsLength = nulls.length;
            int bitsLength = nullsLength * 8;

            BitSet bitset = new BitSet(bitsLength);

            for (int i = 0; i < bitsLength; i++) {
                int nullIndex = i / 8;
                int maskIndex = i % 8;

                byte aNull = nulls[nullIndex];
                byte mask = MASKS[maskIndex];

                boolean isNull = (aNull & mask) != 0;
                bitset.set(i, isNull);
            }

            return bitset;
        }

        public ColumnDataBuilder column(TColumn column) {
            this.column = column;
            return this;
        }

        public ColumnDataBuilder descriptor(ColumnDescriptor columnDescriptor) {
            this.columnDescriptor = columnDescriptor;
            return this;
        }

        public ColumnData build() {
            if (column.isSetBoolVal()) {
                TBoolColumn boolVal = column.getBoolVal();
                return new BooleanColumnData(columnDescriptor, boolVal.getValues(), buildBitSet(boolVal.getNulls()));
            } else if (column.isSetByteVal()) {
                TByteColumn byteVal = column.getByteVal();
                return new ByteColumnData(columnDescriptor, byteVal.getValues(), buildBitSet(byteVal.getNulls()));
            } else if (column.isSetI16Val()) {
                TI16Column i16Val = column.getI16Val();
                return new ShortColumnData(columnDescriptor, i16Val.getValues(), buildBitSet(i16Val.getNulls()));
            } else if (column.isSetI32Val()) {
                TI32Column i32Val = column.getI32Val();
                return new IntegerColumnData(columnDescriptor, i32Val.getValues(), buildBitSet(i32Val.getNulls()));
            } else if (column.isSetI64Val()) {
                TI64Column i64Val = column.getI64Val();
                return new LongColumnData(columnDescriptor, i64Val.getValues(), buildBitSet(i64Val.getNulls()));
            } else if (column.isSetDoubleVal()) {
                TDoubleColumn doubleVal = column.getDoubleVal();
                return new DoubleColumnData(columnDescriptor, doubleVal.getValues(), buildBitSet(doubleVal.getNulls()));
            } else if (column.isSetBinaryVal()) {
                TBinaryColumn binaryVal = column.getBinaryVal();
                return new BinaryColumnData(columnDescriptor, binaryVal.getValues(), buildBitSet(binaryVal.getNulls()));
            } else if (column.isSetStringVal()) {
                TStringColumn stringVal = column.getStringVal();
                return new StringColumnData(columnDescriptor, stringVal.getValues(), buildBitSet(stringVal.getNulls()));
            }

            throw new IllegalStateException(MessageFormat.format("no values set for TColumn [{0}]", column));
        }

    }
}
