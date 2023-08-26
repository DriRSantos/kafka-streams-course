package com.udemy.kafka.streams.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {

    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        // we get a globaltable out of Kafka, the key of our globalKTable is the user ID
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");
        // we get a stream of user purchases
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        // enrich the stream user-purchases with user-table data
        KStream<String, String> userPurchasesEnrichedJoin =
                userPurchases.join(usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
                );
        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // enrich the stream user-purchases using a Left Join with user-table data
        KStream<String, String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> {
                            // is left join, value userInfo can be null
                            if (userInfo != null) {
                                return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                            } else {
                                return "Purchase=" + userPurchase + ",UserInfo=null";
                            }
                        }
                );
        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        return builder.build();
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        UserEventEnricherApp userEventEnricherApp = new UserEventEnricherApp();
        KafkaStreams streams = new KafkaStreams(userEventEnricherApp.createTopology(), props);
        streams.cleanUp(); // do only in dev, never em prod
        streams.start();

        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
