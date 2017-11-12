#! /bin/bash

### Make sure you already run 'inject_mysql.sh' beforehand,
### that script populates the db table.

airport=$1

mysql -uroot -proot << EOF
USE capstone;
select * from departure_delay_by_origin_by_airline
where origin = "$airport"
order by delay
limit 10
EOF
