#! /bin/bash

aws s3 sync s3://daiqian.capstone/group1_question1 .
filename="part-r-00000"
n=$(wc -l < $filename)
echo $n

gnuplot --persist << EOF
  set logscale xy
  set xrange [1:1e8]
  set yrange [0.01:1]
  plot "$filename" using 1:(column(0)/$n)
EOF
