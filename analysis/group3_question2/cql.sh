#! /bin/bash

## TODO in MapReduce code:
## 1. use '|' as delimiter
## 2. get rid of quote in A-B-C key field.

password=iYDdgdNX0Vnu

cqlsh -u cassandra -p ${password} << EOF

CREATE SCHEMA IF NOT EXISTS capstone
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE capstone;

DROP TABLE IF EXISTS connecting_flights;
CREATE TABLE connecting_flights (
  id text PRIMARY KEY,
  delay double
);

COPY connecting_flights FROM 'data.txt' with DELIMITER = '|';
select * from connecting_flights;

EOF
