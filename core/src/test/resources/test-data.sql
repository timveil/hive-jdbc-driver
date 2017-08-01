CREATE TABLE test_data (
  `col_tinyint` tinyint comment 'byte = tinyint',
  `col_smallint` smallint comment 'short = smallint',
  `col_int` int,
  `col_bigint` bigint comment 'long = bigint',
  `col_boolean` boolean,
  `col_float` float,
  `col_double` double,
  `col_string` string,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,10),
  `col_varchar` varchar(10),
  `col_date` date,
  `col_char` char(1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
LOCATION '/tmp/test_data';
