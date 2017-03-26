CREATE TABLE test_table (
  `col_tinyint` tinyint,
  `col_smallint` smallint,
  `col_int` int,
  `col_bigint` bigint,
  `col_boolean` boolean,
  `col_float` float,
  `col_double` double,
  `col_string` string,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,10),
  `col_varchar` varchar(10),
  `col_date` date,
  `col_char` char(1),
  `col_binary` binary
);

insert into table test_table
 values(1,2,3,4,true,1.1,1.2,'test',null,1.3,'xxx',null,'a',null)

insert into table test_table
 values(2,3,4,5,true,1.11,1.22,'test2',null,1.4,'xxxx',null,'b',null)