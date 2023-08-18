package com.udemy.kafka.streams.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - get stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        // 2 - map values to lowercase
        KTable<String,Long> wordCounts = wordCountInput
                .mapValues(textLine -> textLine.toLowerCase())
                // 3 - flatmap values split by space
                .flatMapValues(lowercaseTextLine -> Arrays.asList(lowercaseTextLine.split("\\W+")))
                // 4 - select key to apply a key (old key is discharded)
                .selectKey((ignoredKey, word) -> word)
                // 5 - groupByKey before aggregation
                .groupByKey()
                // 6 - count ocurrences
                .count(Materialized.as("Counts"));

        // 7 - toStream().to - in order to write the results back do Kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        WordCountApp wordCountApp = new WordCountApp();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), props);
        streams.start();

       // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // print the topology every 5 seconds for learning purposes
        while(true){
            streams.metadataForLocalThreads().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
