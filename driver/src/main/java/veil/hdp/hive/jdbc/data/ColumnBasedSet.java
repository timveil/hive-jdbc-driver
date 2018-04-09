package veil.hdp.hive.jdbc.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import veil.hdp.hive.jdbc.Builder;
import veil.hdp.hive.jdbc.bindings.*;
import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.Schema;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class ColumnBasedSet {

    private static final byte[] MASKS = {
            (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08, (byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80
    };

    private static final Logger log = LogManager.getLogger(ColumnBasedSet.class);

    private final int rowCount;
    private final List<ColumnData> columns;

    private ColumnBasedSet(int rowCount, List<ColumnData> columns) {
        this.rowCount = rowCount;
        this.columns = columns;
    }

    public static ColumnBasedSetBuilder builder() {
        return new ColumnBasedSetBuilder();
    }

    public List<ColumnData> getColumns() {
        return columns;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public static class ColumnBasedSetBuilder implements Builder<ColumnBasedSet> {

        private TRowSet rowSet;
        private Schema schema;

        private ColumnBasedSetBuilder() {
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

                boolean isNull = ((int) aNull & (int) mask) != 0;
                bitset.set(i, isNull);
            }

            return bitset;
        }

        public ColumnBasedSetBuilder rowSet(TRowSet tRowSet) {
            this.rowSet = tRowSet;
            return this;
        }

        public ColumnBasedSetBuilder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public ColumnBasedSet build() {

            List<ColumnData> columns = new ArrayList<>(rowSet.getColumnsSize());

            Iterator<TColumn> columnsIterator = rowSet.getColumnsIterator();

            int position = 1;

            int rowCount = -1;

            while (columnsIterator.hasNext()) {

                TColumn column = columnsIterator.next();

                ColumnDescriptor columnDescriptor = schema.getColumn(position);

                if (column.isSetBoolVal()) {
                    TBoolColumn boolVal = column.getBoolVal();
                    columns.add(new BooleanColumnData(columnDescriptor, boolVal.getValues(), buildBitSet(boolVal.getNulls()), boolVal.getValuesSize()));
                } else if (column.isSetByteVal()) {
                    TByteColumn byteVal = column.getByteVal();
                    columns.add(new ByteColumnData(columnDescriptor, byteVal.getValues(), buildBitSet(byteVal.getNulls()), byteVal.getValuesSize()));
                } else if (column.isSetI16Val()) {
                    TI16Column i16Val = column.getI16Val();
                    columns.add(new ShortColumnData(columnDescriptor, i16Val.getValues(), buildBitSet(i16Val.getNulls()), i16Val.getValuesSize()));
                } else if (column.isSetI32Val()) {
                    TI32Column i32Val = column.getI32Val();
                    columns.add(new IntegerColumnData(columnDescriptor, i32Val.getValues(), buildBitSet(i32Val.getNulls()), i32Val.getValuesSize()));
                } else if (column.isSetI64Val()) {
                    TI64Column i64Val = column.getI64Val();
                    columns.add(new LongColumnData(columnDescriptor, i64Val.getValues(), buildBitSet(i64Val.getNulls()), i64Val.getValuesSize()));
                } else if (column.isSetDoubleVal()) {
                    TDoubleColumn doubleVal = column.getDoubleVal();
                    columns.add(new DoubleColumnData(columnDescriptor, doubleVal.getValues(), buildBitSet(doubleVal.getNulls()), doubleVal.getValuesSize()));
                } else if (column.isSetBinaryVal()) {
                    TBinaryColumn binaryVal = column.getBinaryVal();
                    columns.add(new BinaryColumnData(columnDescriptor, binaryVal.getValues(), buildBitSet(binaryVal.getNulls()), binaryVal.getValuesSize()));
                } else if (column.isSetStringVal()) {
                    TStringColumn stringVal = column.getStringVal();
                    columns.add(new StringColumnData(columnDescriptor, stringVal.getValues(), buildBitSet(stringVal.getNulls()), stringVal.getValuesSize()));
                }

                position++;
            }

            if (!columns.isEmpty()) {
                rowCount = columns.get(0).getRowCount();
            }


            return new ColumnBasedSet(rowCount, columns);
        }
    }

}
