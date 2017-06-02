package veil.hdp.hive.jdbc;

public class BinaryZookeeperDataSource extends HiveDataSource {
    @Override
    HiveDriver builDriver() {
        return new BinaryZookeeperHiveDriver();
    }
}
