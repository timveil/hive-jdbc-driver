CREATE TABLE date_time_test (
  `col_date` date,
  `col_timestamp` timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
LOCATION '/tmp/date_time_test';