#! /bin/bash

### Make sure you already run 'inject_mysql.sh' beforehand,
### that script populates the db table.

origin=$1
dest=$2

mysql -uroot -proot << EOF
USE capstone;
select * from arrival_delay_by_origin_by_dest
where origin = "$origin" and dest = "$dest"
EOF
