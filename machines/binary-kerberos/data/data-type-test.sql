CREATE EXTERNAL TABLE IF NOT EXISTS data_type_test (
  `col_tinyint` tinyint comment 'tinyint = byte',
  `col_smallint` smallint comment 'smallint = short',
  `col_int` int,
  `col_bigint` bigint comment 'bigint = long',
  `col_boolean` boolean,
  `col_float` float,
  `col_double` double,
  `col_string` string,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,10) comment 'decimal = BigDecimal',
  `col_varchar` varchar(10),
  `col_date` date,
  `col_char` char(1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
LOCATION '/tmp/data_type_test';
