package veil.hdp.hive.jdbc;

public class HttpZookeeperDataSource extends HiveDataSource {
    @Override
    HiveDriver buildDriver() {
        return new HttpZookeeperHiveDriver();
    }
}
