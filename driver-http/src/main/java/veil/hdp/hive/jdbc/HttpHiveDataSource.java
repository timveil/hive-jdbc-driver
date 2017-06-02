package veil.hdp.hive.jdbc;

public class HttpHiveDataSource extends HiveDataSource {
    @Override
    HiveDriver builDriver() {
        return new HttpHiveDriver();
    }
}
