/* 
* Script that generates a multiplicity of binlog types for for THL checking.
* Run the script as follows on a newly installed master replicator: 
*
*   # Remove csv file owned by MySQL. 
*   sudo rm /tmp/all_mysql_types.csv
*   mysql -uroot < mysql_all_thl_types.sql
*/
use test

/* Start with row events.  DDL will still be statements of course. */
set session binlog_format=row;

/* Statements. */
DROP TABLE IF EXISTS all_mysql_types;
CREATE TABLE `all_mysql_types` (
  `my_id` int PRIMARY KEY AUTO_INCREMENT,
  `my_bit` bit(1) DEFAULT NULL,
  `my_tinyint` tinyint(4) DEFAULT NULL,
  `my_boolean` tinyint(1) DEFAULT NULL,
  `my_smallint` smallint(6) DEFAULT NULL,
  `my_mediumint` mediumint(9) DEFAULT NULL,
  `my_int` int(11) DEFAULT NULL,
  `my_bigint` bigint(20) DEFAULT NULL,
  `my_decimal_10_5` decimal(10,5) DEFAULT NULL,
  `my_float` float DEFAULT NULL,
  `my_double` double DEFAULT NULL,
  `my_date` date DEFAULT NULL,
  `my_datetime` datetime DEFAULT NULL,
  `my_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `my_time` time DEFAULT NULL,
  `my_year` year(4) DEFAULT NULL,
  `my_char_10` char(10) DEFAULT NULL,
  `my_varchar_10` varchar(10) DEFAULT NULL,
  `my_tinytext` tinytext,
  `my_text` text,
  `my_mediumtext` mediumtext,
  `my_longtext` longtext,
  `my_enum_abc` enum('a','b','c') DEFAULT NULL,
  `my_set_def` set('d','e','f') DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/* Row insert with two rows. */
INSERT INTO all_mysql_types(
    my_bit, my_tinyint, my_boolean, my_smallint, my_mediumint,
    my_int, my_bigint, my_decimal_10_5, my_float, my_double, my_date,
    my_datetime, my_timestamp, my_time, my_year, my_char_10, my_varchar_10,
    my_tinytext, my_text, my_mediumtext, my_longtext, my_enum_abc,
    my_set_def)
  VALUES (
    1, /* bit(1) DEFAULT NULL, */
    5, /* `my_tinyint` tinyint(4) DEFAULT NULL, */
    false, /* `my_boolean` tinyint(1) DEFAULT NULL */
    500, /* `my_smallint` smallint(6) DEFAULT NULL */
    50000, /* `my_mediumint` mediumint(9) DEFAULT NULL */
    50000000, /* `my_int` int(11) DEFAULT NULL */
    6666666, /* `my_bigint` bigint(20) DEFAULT NULL */
    10.5, /* `my_decimal_10_5` decimal(10,5) DEFAULT NULL */
    10.55, /* `my_float` float DEFAULT NULL */
    500.11, /* `my_double` double DEFAULT NULL */
    '2014-01-31', /* `my_date` date DEFAULT NULL */
    '2014-01-30 01:30:02', /* `my_datetime` datetime DEFAULT NULL */
    NOW(), /* `my_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP */
    '11:12:13', /* `my_time` time DEFAULT NULL */
    '2013', /* `my_year` year(4) DEFAULT NULL */
    'abcdef', /* `my_char_10` char(10) DEFAULT NULL */
    'abcdefgh', /* `my_varchar_10` varchar(10) DEFAULT NULL */
    'tiny text', /* `my_tinytext` tinytext */
    'text', /* `my_text` text */
    'medium text', /* `my_mediumtext` mediumtext */
    'long text', /* `my_longtext` longtext */
    'b', /* `my_enum_abc` enum('a','b','c') DEFAULT NULL */
    'e' /* `my_set_def` set('d' 'e','f') DEFAULT NULL */
  ), (
    1, /* bit(1) DEFAULT NULL, */
    5, /* `my_tinyint` tinyint(4) DEFAULT NULL, */
    false, /* `my_boolean` tinyint(1) DEFAULT NULL */
    500, /* `my_smallint` smallint(6) DEFAULT NULL */
    50000, /* `my_mediumint` mediumint(9) DEFAULT NULL */
    50000000, /* `my_int` int(11) DEFAULT NULL */
    6666666, /* `my_bigint` bigint(20) DEFAULT NULL */
    10.5, /* `my_decimal_10_5` decimal(10,5) DEFAULT NULL */
    10.55, /* `my_float` float DEFAULT NULL */
    500.11, /* `my_double` double DEFAULT NULL */
    '2014-01-31', /* `my_date` date DEFAULT NULL */
    '2014-01-30 01:30:02', /* `my_datetime` datetime DEFAULT NULL */
    NOW(), /* `my_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP */
    '11:12:13', /* `my_time` time DEFAULT NULL */
    '2013', /* `my_year` year(4) DEFAULT NULL */
    'abcdef', /* `my_char_10` char(10) DEFAULT NULL */
    'abcdefgh', /* `my_varchar_10` varchar(10) DEFAULT NULL */
    'tiny text', /* `my_tinytext` tinytext */
    'text', /* `my_text` text */
    'medium text', /* `my_mediumtext` mediumtext */
    'long text', /* `my_longtext` longtext */
    'b', /* `my_enum_abc` enum('a','b','c') DEFAULT NULL */
    'e' /* `my_set_def` set('d' 'e','f') DEFAULT NULL */
);
SELECT * FROM all_mysql_types;

/* Row update on 1st row of the table. */
UPDATE all_mysql_types 
  SET my_bit=0, my_decimal_10_5=11.6
    WHERE my_id=1;
SELECT * FROM all_mysql_types;

/* Row delete 2nd row of table. */
DELETE FROM all_mysql_types 
  WHERE my_id=2;
SELECT * FROM all_mysql_types;

/* Switch to statement replication and generate a CSV file FROM the table. */
set session binlog_format=statement;

SELECT * INTO OUTFILE '/tmp/all_mysql_types.csv' FROM all_mysql_types;

/* Truncate the table and reload the file. */
TRUNCATE all_mysql_types;
LOAD DATA INFILE '/tmp/all_mysql_types.csv' INTO TABLE all_mysql_types;
