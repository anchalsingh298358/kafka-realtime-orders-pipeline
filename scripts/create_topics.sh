#!/bin/bash

echo "Creating Kafka topics..."

docker exec kafka kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server localhost:29092 \
  --partitions 3 \
  --replication-factor 1


docker exec kafka kafka-topics \
  --create \
  --topic orders_dlq \
  --bootstrap-server localhost:29092 \
  --partitions 1 \
  --replication-factor 1


echo "Topics created."