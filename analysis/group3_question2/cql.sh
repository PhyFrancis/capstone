#! /bin/bash

## Caveat: (The following are true unless allow filtering, which sucks)
## 1. Cassandra only allows query on primary key
## 2. When using compound primary key, the column can only be restricted if
##    preceeding column is restricted.
## 3. range query is not supported.

password=iYDdgdNX0Vnu

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
  PRIMARY KEY ((date), airport_a, airport_b, airport_c)
);

COPY connecting_flights FROM 'data.txt' with DELIMITER = '|';
SELECT * from connecting_flights WHERE date = '2008-08-01' AND airport_a = 'ORD';

EOF
