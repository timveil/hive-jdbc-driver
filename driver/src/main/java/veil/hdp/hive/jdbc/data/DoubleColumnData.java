package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;

import java.util.BitSet;
import java.util.List;

import static veil.hdp.hive.jdbc.metadata.HiveType.FLOAT;

class DoubleColumnData extends AbstractColumnData<Double> {
    DoubleColumnData(ColumnDescriptor descriptor, List<Double> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {

        HiveType doubleType = getDescriptor().getColumnType().getHiveType();

        Double value = getValue(row);

        if (doubleType == FLOAT) {
            return new FloatColumn(value == null ? null : new Float(value));
        } else {
            return new DoubleColumn(value);
        }

    }
}
