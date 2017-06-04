package veil.hdp.hive.jdbc;

public class BinaryHiveDataSource extends HiveDataSource {

    @Override
    HiveDriver buildDriver() {
        return new BinaryHiveDriver();
    }
}
