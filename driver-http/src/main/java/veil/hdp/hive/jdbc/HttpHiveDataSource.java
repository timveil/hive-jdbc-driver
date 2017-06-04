package veil.hdp.hive.jdbc;

public class HttpHiveDataSource extends HiveDataSource {
    @Override
    HiveDriver buildDriver() {
        return new HttpHiveDriver();
    }
}
