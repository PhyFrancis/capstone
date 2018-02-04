#! /bin/bash

### WARNING: This will wipe out all existing data in Cassandra.

date=$1
airport_a=$2
airport_b=$3
airport_c=$4

password=RnuR4XyPrI0R

cqlsh -u cassandra -p ${password} << EOF
USE capstone;
SELECT * from connecting_flights 
WHERE date = '$date' 
AND airport_a = '$airport_a'
AND airport_b = '$airport_b'
AND airport_c = '$airport_c'
LIMIT 1;
EOF
