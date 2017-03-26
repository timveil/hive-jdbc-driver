package veil.hdp.hive.jdbc;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ColumnData<T> {


    private static final byte[] MASKS = new byte[]{
            0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80
    };

    private final List<T> values;
    private final HiveType hiveType;

    public ColumnData(HiveType hiveType, List<T> originalValues, byte[] nulls) {

        this.hiveType = hiveType;

        BitSet bitset = new BitSet();
        int bits = nulls.length * 8;
        for (int i = 0; i < bits; i++) {
            bitset.set(i, (nulls[i / 8] & MASKS[i % 8]) != 0);
        }


        List<T> cleanValues = new ArrayList<>();

        for (int i = 0; i < originalValues.size(); i++) {
            if (bitset.get(i)) {
                cleanValues.add(null);
            } else {
                cleanValues.add(originalValues.get(i));
            }
        }

        this.values = cleanValues;

    }

    public T getValue(int index) {
        return values.get(index);
    }

    public int getRowCount() {
        return values.isEmpty() ? 0 : values.size();
    }

    public List<T> getValues() {
        return values;
    }

    public HiveType getHiveType() {
        return hiveType;
    }

}
