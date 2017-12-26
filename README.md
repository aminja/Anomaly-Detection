# Anomaly Detection using Spark + Kafka
This project is provided to illustrate the concepts of stream processing using big data tools. We use Apache Kafka to stream the input file and Apache Spark to process the produced stream.

## Run in docker mode
- clone repository

```
git clone https://gitlab.com/aminja/Anomaly-Detection.git
cd Anomaly-Detection/
```

- run Docker containers


```
docker-compose up java
```

Note: Java container calls startSpark.sh which need spark engine to run.

If there were no `spark-2.2.0-bin-hadoop2.7/` directory in `build/` path (working directory of java), startSpark.sh would download it from https://spark.apache.org/. So you can bypass it by providing the spark in this path.

- pipe data into kafka

```
./build/setupPipe.sh
```
## Manuall Installation
- create jar file:

```
sbt assembly
```

- run dependant services (zookeeper + Kafka) by confluent (3rd party platform)
```
confluent start Kafka
```

- run compiled jar file with a standalone spark
```
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="cpulogs" \
spark-2.2.0-bin-hadoop2.7/bin/spark-submit --master local[*] --jars spark-sql-kafka-0-10_2.11-2.1.0.jar OutlierDetector-assembly-1.0.jar
```


## Built With

* [Kafka](https://kafka.apache.org/) - Distributed streaming platform
* [Spark](https://spark.apache.org/) - Large-scale data processing engine

## License

This project is licensed under the MIT License.


