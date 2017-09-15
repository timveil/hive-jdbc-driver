package veil.hdp.hive.jdbc.data;

public interface ColumnData {
    Column getColumn(int row);

    int getRowCount();
}
