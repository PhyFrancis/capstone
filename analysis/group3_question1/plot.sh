#! /bin/bash

aws s3 sync s3://daiqian.capstone/group1_question1 data
filepath="data/part-r-00000"
n=$(wc -l < $filepath)
echo $n

gnuplot --persist << EOF
  set logscale xy
  set xrange [1:1e8]
  set yrange [0.01:1]
  plot "$filepath" using 1:(column(0)/$n)
EOF
