#!/bin/bash

if [ ! -d "spark-2.2.0-bin-hadoop2.7/" ]; then
   wget -c http://www-eu.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
   tar -xvzf spark-2.2.0-bin-hadoop2.7.tgz
else
   echo "Spark is available"
fi

echo "Spark: Waiting 10 seconds ..."
sleep 10

KAFKA_BROKERS="kafka:9092" \
KAFKA_TOPIC="cpulogs" \
spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master local[*] --jars spark-sql-kafka-0-10_2.11-2.1.0.jar OutlierDetector-assembly-1.0.jar
