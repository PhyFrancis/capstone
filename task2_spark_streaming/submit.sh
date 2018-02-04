#! /bin/bash

spark_home=/home/ubuntu/spark-2.2.1-bin-hadoop2.7

# hdfs dfs -rm -r /result/g1q1
# ${spark_home}/bin/spark-submit \
#   --class G1Q1 \
#   --master spark://ip-172-31-5-186.ec2.internal:6066 \
#   --deploy-mode cluster \
#   hdfs://ip-172-31-5-186:9000/jars/g1q1_2.11-1.0.jar \
#   hdfs://ip-172-31-5-186:9000/cleaned_data/* \
#   hdfs://ip-172-31-5-186:9000/result/g1q1

${spark_home}/bin/spark-submit \
  --class G1Q1 \
  --master local[4] \
  ~/spark-2.2.1-bin-hadoop2.7/work/g1q1/target/scala-2.11/g1q1_2.11-1.0.jar \
  /home/ubuntu/spark-2.2.1-bin-hadoop2.7/work aaa
