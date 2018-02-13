#! /bin/bash

stop_kafka=false
start_kafka=false

topic_name=cleaned_data
kafka_home=/home/ubuntu/kafka_2.11-1.0.0
hdfs_data_path=/test_data/cleaned_data

if [ ${stop_kafka} = true ] ; then
  echo Stoping kafka broker...
  ${kafka_home}/bin/kafka-server-stop.sh && sleep 3
  echo Stoping zookeeper...
  ${kafka_home}/bin/zookeeper-server-stop.sh && sleep 3
fi

if [ ${start_kafka} = true ] ; then
  echo Starting zookeeper
  ${kafka_home}/bin/zookeeper-server-start.sh ${kafka_home}/config/zookeeper.properties &
  sleep 5
  echo Starting broker
  ${kafka_home}/bin/kafka-server-start.sh ${kafka_home}/config/server.properties &
  sleep 5
  echo Creating topic ${topic_name}
  ${kafka_home}/bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic ${topic_name}
fi

for filename in $(hdfs dfs -ls ${hdfs_data_path} | sed '1d;s/  */ /g' | cut -d\  -f8) ; do
  echo fanning out ${filename}
  hdfs dfs -cat ${filename} \
    | ${kafka_home}/bin/kafka-console-producer.sh \
        --broker-list localhost:9092 \
        --topic ${topic_name}
done
