#! /bin/bash

# Prerequisite:
#   1. hadoop cluster is up, namenode is at hdfs://ip-172-31-5-186:9000
#   2. spark cluster is up.

spark_home=/home/ubuntu/spark-2.2.1-bin-hadoop2.7
jar_local=/home/ubuntu/spark-2.2.1-bin-hadoop2.7/work/capstone/task2_spark_streaming/target/scala-2.11/capstonetask2_2.11-1.0.jar
jar_hdfs=hdfs://ip-172-31-5-186:9000/jars/capstonetask2_2.11-1.0.jar
cassandra_flag="--packages datastax:spark-cassandra-connector:2.0.1-s_2.11"

g1q1=true
data_clean=false

if [ "${g1q1}" = true ] ; then
  ${spark_home}/bin/spark-submit \
    ${cassandra_flag} \
    --class capstone.G1Q1 \
    --master local[4] \
    ${jar_local} \
    ${PWD}/tmp_input ${PWD}/tmp_output
    # --master spark://ip-172-31-5-186.ec2.internal:6066 \
    # --deploy-mode cluster \
    # hdfs://ip-172-31-5-186:9000/jars/g1q1_2.11-1.0.jar \
    # hdfs://ip-172-31-5-186:9000/cleaned_data/* \
    # hdfs://ip-172-31-5-186:9000/result/g1q1
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
