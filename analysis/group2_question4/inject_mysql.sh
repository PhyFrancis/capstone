#! /bin/bash

### pre-step: download the data from aws s3 by running
### $ aws configure [one time setup for credential]
### $ aws s3 sync s3://daiqian.capstone/group2_question1 .

table_name=arrival_delay_by_origin_by_dest
data_src=$(pwd)/data

mysql -uroot -proot << EOF
CREATE DATABASE IF NOT EXISTS capstone;
USE capstone;
DROP TABLE IF EXISTS $table_name;
CREATE TABLE $table_name (
  origin CHAR(20),
  dest CHAR(20),
  delay DECIMAL(10,6)
);
EOF

for f in $(ls $data_src) ; do
  cat $data_src/$f | awk 'BEGIN {FPAT="([^ \t]+)|(\"[^\"]+\")"} {print $1,$2,$4}' > tmp

mysql -uroot -proot << EOF
USE capstone;
LOAD DATA LOCAL INFILE "$(pwd)/tmp"
INTO TABLE $table_name
COLUMNS TERMINATED BY ' '
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';
EOF

done

rm -f tmp
