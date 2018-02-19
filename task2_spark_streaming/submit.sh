#! /bin/bash

# Prerequisite:
#   1. hadoop cluster is up, namenode is at hdfs://ip-172-31-5-186:9000
#   2. spark cluster is up, master url is at spark://ip-172-31-5-186.ec2.internal:6066
#   3. cassandra cluster is up.

spark_home=/home/ubuntu/spark-2.2.1-bin-hadoop2.7
jar_local=/home/ubuntu/spark-2.2.1-bin-hadoop2.7/work/capstone/task2_spark_streaming/target/scala-2.11/CapstoneTask2-assembly-1.0.jar
jar_hdfs=hdfs://ip-172-31-5-186:9000/jars/CapstoneTask2-assembly-1.0.jar
cassandra_flag="--packages datastax:spark-cassandra-connector:2.0.1-s_2.11"

g3q2=true # verified
g2q4=false # verified
g2q2=false # verified
g2q1=false # verified
g1q2=false # verified
g1q1=false # verified
data_clean=false

if [ "${g3q2}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G3Q2 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${g2q4}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G2Q4 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${g2q2}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G2Q2 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${g2q1}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G2Q1 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${g1q2}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G1Q2 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${g1q1}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.G1Q1 \
    --master spark://ip-172-31-5-186.ec2.internal:6066 --deploy-mode cluster \
    ${jar_hdfs}
fi

if [ "${data_clean}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    --class capstone.DataCleaner \
    --master spark://ip-172-31-5-186.ec2.internal:6066 \
    --deploy-mode cluster \
    ${jar_hdfs} \
    hdfs://ip-172-31-5-186:9000/raw_data/* \
    hdfs://ip-172-31-5-186:9000/cleaned_data
fi
