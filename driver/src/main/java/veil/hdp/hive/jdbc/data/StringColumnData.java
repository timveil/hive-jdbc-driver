package veil.hdp.hive.jdbc.data;

import veil.hdp.hive.jdbc.metadata.ColumnDescriptor;
import veil.hdp.hive.jdbc.metadata.HiveType;
import veil.hdp.hive.jdbc.utils.SqlDateTimeUtils;

import java.math.BigDecimal;
import java.util.BitSet;
import java.util.List;

import static veil.hdp.hive.jdbc.metadata.HiveType.*;

class StringColumnData extends AbstractColumnData<String> {

    StringColumnData(ColumnDescriptor descriptor, List<String> values, BitSet nulls, int rowCount) {
        super(descriptor, values, nulls, rowCount);
    }

    @Override
    public Column getColumn(int row) {

        HiveType stringType = getDescriptor().getColumnType().getHiveType();

        String value = getValue(row);

        if (stringType == DECIMAL) {
            return new DecimalColumn(value == null ? null : new BigDecimal(value));
        } else if (stringType == CHAR) {
            return new CharacterColumn(value == null ? null : value.charAt(0));
        } else if (stringType == VARCHAR) {
            return new VarcharColumn(value == null ? null : value);
        } else if (stringType == TIMESTAMP) {
            return new TimestampColumn(value == null ? null : SqlDateTimeUtils.convertStringToTimestamp(value));
        } else if (stringType == DATE) {
            return new DateColumn(value == null ? null : SqlDateTimeUtils.convertStringToDate(value));
        } else {
            return new StringColumn(value);
        }
    }
}
