#!/bin/bash

# create input topic with one partition to get full ordering
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions
kafka-topics.sh --create --bootstrap-server [::1]:9092 --replication-factor 1 --partitions 1 --topic bank-transactions

# create output log compacted topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-exactly-once --config cleanup.policy=compact
kafka-topics.sh --create --bootstrap-server [::1]:9092 --replication-factor 1 --partitions 1 --topic bank-balance-exactly-once --config cleanup.policy=compact

# launch a Kafka consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic bank-balance-exactly-once \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# wsls
    kafka-console-consumer.sh --bootstrap-server [::1]:9092 \
    --topic bank-balance-exactly-once \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# also lauch a consumer on bank-transactions topic
    kafka-console-consumer.sh --bootstrap-server [::1]:9092 --topic bank-transactions --from-beginning