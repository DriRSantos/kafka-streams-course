package com.udemy.kafka.streams.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.util.Properties;

public class FavouriteColour {
    public Topology createTopology() {
        // StreamsBuilder to build KStream and KTable instances
        StreamsBuilder builder = new StreamsBuilder();
        // create Stream topic of String and String, with all keys null
        KStream<String, String> favoriteColours = builder.stream("favorite-colours-input");
        // filter to ensure that a comma exist to be splitted
        favoriteColours.filter((key, value) -> value.contains(","))
                // select userId as key, always put lowercase
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // get the color from the value
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // ensure the correct colors
                .filter((userId, color) -> color.equals("green") || color.equals("red") || color.equals("blue"))
                // or use .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));
                // write the results back to Kafka, not need Serdes they're default (intermediary topic)
                .to("user-keys-and-colours");

        // read from Kafka as KTable
        KTable<String, String> coloursCount = builder.table("user-keys-and-colours");
        // group by color newKey and count
        KTable<String, Long> favouriteColors = coloursCount.groupBy((userId, color) -> new KeyValue<>(color, color))
        // count keys colors, attention to Serdes
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                        "CountsByColors")  /* table/store name */
                        .withKeySerde(Serdes.String()) /* key serde */
                        .withValueSerde(Serdes.Long()) /* value serde */
                );

        // write the results back to Kafka
        favouriteColors.toStream().to("favorite-colours-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        // create properties of app Kafka Stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-colour-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // set cache to 0, just to show every step in the transformation
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");

        // instance of FavouriteColour to create topology
        FavouriteColour favoriteColour = new FavouriteColour();
        // create KafkaStreams passing topology and properties, starts
        KafkaStreams streams = new KafkaStreams(favoriteColour.createTopology(), props);
        // clean up of the local StateStore directory
        streams.cleanUp();
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // print the topology
        streams.metadataForLocalThreads().forEach(data -> System.out.println(data));
    }
}