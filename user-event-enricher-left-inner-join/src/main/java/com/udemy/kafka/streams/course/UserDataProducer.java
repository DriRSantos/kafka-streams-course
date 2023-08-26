package com.udemy.kafka.streams.course;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks important to receiv confirmation from leader and ISRs
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // only on development
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent Producer, ensure we don't push duplicates
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // DO NOT DO THIS ON PRODUCTION OR IN ANY PRODUCER, NEVER, IS JUST TO SHOW THE JOIN
        // 1 - create A new user, then send some data to Kafka
        System.out.println("\nExample 1 - new user\n");
        producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        // 2 - receive user purchase, but it doesn't exist in Kafka
        System.out.println("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

        Thread.sleep(10000);

        // 3 - update user "john", and send a new transaction
        System.out.println("\nExample 3 - update to user\n");
        producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);

        // 4 - send a user purchase for stephane, but it exists in Kafka later
        System.out.println("\nExample 4 - non existing user then user\n");
        producer.send(purchaseRecord("stephane", "Computer (4)")).get();
        producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
        producer.send(purchaseRecord("stephane", "Books (4)")).get();
        producer.send(userRecord("stephane", null)).get(); // delete for cleanup

        Thread.sleep(10000);

        // 5 - create a user, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("dri", "First=Dri")).get();
        producer.send(userRecord("dri", null)).get(); // that's the delete record
        producer.send(purchaseRecord("dri", "Apache Kafka Series (5)")).get();

        Thread.sleep(10000);

        System.out.println("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }
    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<>("user-purchases", key, value);
    }
}