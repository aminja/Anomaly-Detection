#!/bin/bash

docker exec -i $(docker-compose ps -q kafka) kafka-console-producer.sh --broker-list localhost:9092 --topic cpulogs < cpu_usage.data


