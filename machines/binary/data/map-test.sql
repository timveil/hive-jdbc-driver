CREATE EXTERNAL TABLE IF NOT EXISTS map_test(
  code int,
  area_name string,
  map_col map<int,string>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY ':'
LOCATION '/tmp/map_test';
