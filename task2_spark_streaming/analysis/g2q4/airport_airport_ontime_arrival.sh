#! /bin/bash

origin=$1
dest=$2

cqlsh=/home/ubuntu/apache-cassandra-3.11.1/bin/cqlsh
table=capstone.origin_dest_ontime_arrival

${cqlsh} << EOF > tmp
select * from $table where origin in ('${origin}') and dest in ('${dest}');
EOF

head -3 tmp
lines=$(wc -l tmp | cut -f 1 -d ' ')
tail -$((lines - 3)) tmp | head -$((lines - 5)) | sort -t '|' -k 3 -n
rm tmp
