#! /bin/bash

### pre-step: download the data from aws s3 by running
### $ aws configure [one time setup for credential]
### $ aws s3 sync s3://daiqian.capstone/group2_question1 .

table_name="departure_delay_by_origin_by_airline"
data_file="/home/daiqianzhang/workspace/analysis/g2q1/data/part-r-00000"

mysql -uroot -proot << EOF

USE capstone;

DROP TABLE IF EXISTS $table_name;
CREATE TABLE $table_name (
  origin CHAR(20),
  airline CHAR(20),
  delay DECIMAL(10,6)
);

LOAD DATA INFILE "$data_file"
INTO TABLE $table_name 
COLUMNS TERMINATED BY ' '
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT * from $table_name;

EOF
