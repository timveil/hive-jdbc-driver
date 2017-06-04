package veil.hdp.hive.jdbc;

public class BinaryZookeeperDataSource extends HiveDataSource {
    @Override
    HiveDriver buildDriver() {
        return new BinaryZookeeperHiveDriver();
    }
}
