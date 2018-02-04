#! /bin/bash

spark_home=/home/ubuntu/spark-2.2.1-bin-hadoop2.7
jar_local=/home/ubuntu/spark-2.2.1-bin-hadoop2.7/work/capstone/task2_spark_streaming/target/scala-2.11/capstonetask2_2.11-1.0.jar
jar_hdfs=hdfs://ip-172-31-5-186:9000/jars/capstonetask2_2.11-1.0.jar

# Data cleaning job
${spark_home}/bin/spark-submit \
  --class DataCleaner \
  --master spark://ip-172-31-5-186.ec2.internal:6066 \
  --deploy-mode cluster \
  ${jar_hdfs} \
  hdfs://ip-172-31-5-186:9000/raw_data/* \
  hdfs://ip-172-31-5-186:9000/cleaned_data

# hdfs dfs -rm -r /result/g1q1
# ${spark_home}/bin/spark-submit \
#   --class G1Q1 \
#   --master spark://ip-172-31-5-186.ec2.internal:6066 \
#   --deploy-mode cluster \
#   hdfs://ip-172-31-5-186:9000/jars/g1q1_2.11-1.0.jar \
#   hdfs://ip-172-31-5-186:9000/cleaned_data/* \
#   hdfs://ip-172-31-5-186:9000/result/g1q1
