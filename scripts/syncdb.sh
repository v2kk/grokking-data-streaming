#!/bin/bash

topic=$1

export HADOOP_CONF_DIR=/etc/hadoop/conf

~/lab/tools/spark-2.0.2-bin-hadoop2.7/bin/spark-submit --class grokking.data.batch.storage.KafkaToPostgresql --master yarn --deploy-mode cluster --queue production --driver-memory 512m --executor-memory 512m --executor-cores 1 --num-executors 1 --conf spark.yarn.jars=hdfs:///user/spark/share/lib/lib_20161027014545/*.jar ~/grokking/bundle/streaming/datastorage/lib/grokking-data-streaming.jar $topic