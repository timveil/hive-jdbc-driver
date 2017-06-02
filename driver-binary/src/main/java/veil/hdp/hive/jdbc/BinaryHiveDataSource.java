package veil.hdp.hive.jdbc;

public class BinaryHiveDataSource extends HiveDataSource {

    @Override
    HiveDriver builDriver() {
        return new BinaryHiveDriver();
    }
}
