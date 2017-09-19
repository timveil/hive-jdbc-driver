CREATE EXTERNAL TABLE IF NOT EXISTS struct_test(
  code int,
  area_name string,
  male_0_4 STRUCT<num:double, total:double, perc:double>,
  male_5_9 STRUCT<num:double, total:double, perc:double>,
  male_10_14 STRUCT<num:double, total:double, perc:double>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
COLLECTION ITEMS TERMINATED BY '#'
LOCATION '/tmp/struct_test';
