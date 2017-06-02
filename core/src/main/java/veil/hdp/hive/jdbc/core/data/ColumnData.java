package veil.hdp.hive.jdbc.core.data;

import veil.hdp.hive.jdbc.core.Builder;
import veil.hdp.hive.jdbc.core.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.core.thrift.TColumn;

import java.text.MessageFormat;
import java.util.BitSet;
import java.util.List;

public class ColumnData<T> {

    private static final byte[] MASKS = new byte[]{
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
                return new BooleanColumnData(columnDescriptor, column.getBoolVal().getValues(), buildBitSet(column.getBoolVal().getNulls()));
            } else if (column.isSetByteVal()) {
                return new ByteColumnData(columnDescriptor, column.getByteVal().getValues(), buildBitSet(column.getByteVal().getNulls()));
            } else if (column.isSetI16Val()) {
                return new ShortColumnData(columnDescriptor, column.getI16Val().getValues(), buildBitSet(column.getI16Val().getNulls()));
            } else if (column.isSetI32Val()) {
                return new IntegerColumnData(columnDescriptor, column.getI32Val().getValues(), buildBitSet(column.getI32Val().getNulls()));
            } else if (column.isSetI64Val()) {
                return new LongColumnData(columnDescriptor, column.getI64Val().getValues(), buildBitSet(column.getI64Val().getNulls()));
            } else if (column.isSetDoubleVal()) {
                return new DoubleColumnData(columnDescriptor, column.getDoubleVal().getValues(), buildBitSet(column.getDoubleVal().getNulls()));
            } else if (column.isSetBinaryVal()) {
                return new BinaryColumnData(columnDescriptor, column.getBinaryVal().getValues(), buildBitSet(column.getBinaryVal().getNulls()));
            } else if (column.isSetStringVal()) {
                return new StringColumnData(columnDescriptor, column.getStringVal().getValues(), buildBitSet(column.getStringVal().getNulls()));
            }

            throw new IllegalStateException(MessageFormat.format("no values set for TColumn [{0}]", column));
        }

    }
}
