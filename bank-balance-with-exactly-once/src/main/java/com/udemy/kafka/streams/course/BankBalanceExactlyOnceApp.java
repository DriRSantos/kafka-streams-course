package com.udemy.kafka.streams.course;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnceApp {

    public Topology createTopology() {
        // SerDes JsonSerializer and JsonDeserializer
        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        // KStream instance, build stream with topic name (same of Producer) and key/value serdes
        KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions",
                Consumed.with(Serdes.String(), jsonNodeSerde));

        // create initial json object for balances, use on aggregate
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        // create Kafka Streams topology
        KTable<String, JsonNode> bankBalance = bankTransactions
                // topic has the key client
                .groupByKey(Grouped.with(Serdes.String(), jsonNodeSerde))
                // aggregate using initialBalance
                .aggregate(
                        () -> initialBalance,
                        // aggregator
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        // state store name
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                // SerDe key e aggValue
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonNodeSerde)
                        );

        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonNodeSerde));

        return builder.build();
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliets");
        // important because sum and count are not idempotent
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // disable cache just to demonstrate steps (DON'T DO ON PRODUCTION)
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");

        // instantiante BankBalanceExactlyOnceApp to call createTopology
        BankBalanceExactlyOnceApp bankBalanceExactlyOnceApp = new BankBalanceExactlyOnceApp();
        KafkaStreams streams = new KafkaStreams(bankBalanceExactlyOnceApp.createTopology(), props);

        // cleanup only on development
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.metadataForLocalThreads().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
