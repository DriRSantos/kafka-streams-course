# download kafka at  https:\\www.apache.org\dyn\closer.cgi?path=\kafka\0.11.0.0\kafka_2.11-0.11.0.0.tgz
# extract kafka in a folder

# WINDOWS ONLY

# open a shell - zookeeper is at localhost:2181
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
## ubuntu wsl2
zookeeper-server-start.sh ~/kafka_2.13-3.1.0/config/zookeeper.properties

# open another shell - kafka is at localhost:9092 (use one of the two below)
bin\windows\kafka-server-start.bat config\server.properties
## ubuntu wsl2
kafka-server-start.sh ~/kafka_2.13-3.1.0/config/server.properties

# create input topic
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
## ubuntu wsl2
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
kafka-topics.sh --create --bootstrap-server [::1]:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input

# create output topic
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-output
## ubuntu wsl2
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-wordcount-output
kafka-topics.sh --create --bootstrap-server [::1]:9092 --replication-factor 1 --partitions 1 --topic streams-wordcount-output

# start a kafka producer
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic streams-plaintext-input
## ubuntu wsl2
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input
kafka-console-producer.sh --bootstrap-server [::1]:9092 --topic streams-plaintext-input

# enter
kafka streams udemy
kafka data processing
kafka streams course
# exit

# verify the data has been written
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic streams-plaintext-input --from-beginning
## ubuntu wsl2
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input --from-beginning
kafka-console-consumer.sh --bootstrap-server [::1]:9092 --topic streams-plaintext-input --from-beginning

# start a consumer on the output topic
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic streams-wordcount-output ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


kafka-console-consumer.sh --bootstrap-server [::1]:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# start the streams application
bin\windows\kafka-run-class.bat org.apache.kafka.streams.examples.wordcount.WordCountDemo

bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo

# verify the data has been written to the output topic!
