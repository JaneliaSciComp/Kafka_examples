# Kafka_examples

This repository contains sample programs for use with Kafka. Unless specified,
all programs use the "test" topic on the production Kafka server.

Producers
---------
produce.py: writes a message to the test topic every 10 seconds

Consumers
---------
consumer.py: uses the kafka library to read all messages from the test topic
fastconsumer.py: uses the confluent_kafka library to read current messages from the test topic
pykafkaconsumer.py: uses the pykafka library to read all messages from the test topic
