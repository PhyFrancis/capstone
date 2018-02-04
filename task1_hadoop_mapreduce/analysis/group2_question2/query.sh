#! /bin/bash

### Make sure you already run 'inject_mysql.sh', that script populates the db
### table.

origin=$1

mysql -uroot -proot << EOF
USE capstone;
select * from departure_delay_by_origin_by_dest
where origin = "$origin"
order by delay
limit 10
EOF
