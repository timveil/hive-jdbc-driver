CREATE EXTERNAL TABLE IF NOT EXISTS array_test(
  code int,
  area_name string,
  array_col array<string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
COLLECTION ITEMS TERMINATED BY '|'
LOCATION '/tmp/array_test';
