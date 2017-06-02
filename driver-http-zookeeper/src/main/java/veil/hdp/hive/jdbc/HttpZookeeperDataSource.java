package veil.hdp.hive.jdbc;

public class HttpZookeeperDataSource extends HiveDataSource {
    @Override
    HiveDriver builDriver() {
        return new HttpZookeeperHiveDriver();
    }
}
