#! /bin/bash

### WARNING: This will wipe out all existing data in Cassandra.

password=G7G1qgN706XI

# 1. create schema.
cqlsh -u cassandra -p ${password} << EOF
CREATE SCHEMA IF NOT EXISTS capstone
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE capstone;
DROP TABLE IF EXISTS connecting_flights;
CREATE TABLE connecting_flights (
  date timestamp,
  airport_a text,
  airport_b text,
  airport_c text,
  delay double,
  first_leg text,
  second_leg text,
  PRIMARY KEY ((date), airport_a, airport_b, airport_c, delay)
);
EOF

# 2. populate table.
tmp_file="tmp.txt"
for idx in {00..36} ; do
filename="s3://daiqian.capstone/group3_question2/unsorted/part-r-000${idx}"
echo copying $filename into $tmp_file
aws s3 cp $filename $tmp_file
echo $filename is now copied into $tmp_file
cqlsh -u cassandra -p ${password} << EOF
USE capstone;
COPY connecting_flights FROM '$tmp_file' with DELIMITER = '|';
EOF
done
rm -f $tmp_file

