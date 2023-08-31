package com.udemy.kafka.streams.course;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertEquals;

public class WordCountAppTest {
    TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setUpTopologyTestDriver(){
        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // instantiate wordCountApp to use createTopology function
        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, props);
    }
    @After
    public void closeTestDriver(){
        testDriver.close();
    }
    public void pushNewInputRecord(String value) {
        // setup test topics
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("word-count-input", stringSerde.serializer(), stringSerde.serializer());
        inputTopic.pipeInput("null", value);
    }
    public TestOutputTopic<String, Long> outputTopic(){
        return testDriver.createOutputTopic("word-count-output", stringSerde.deserializer(), longSerde.deserializer());
    }
    @Test
    public void dummyTest(){
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }
    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);

        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("testing", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("streams", 1L)));

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        for (String s : Arrays.asList("testing", "kafka")) {
            assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>(s, 2L)));
        }
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("again", 1L)));
    }
    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka kafka";
        pushNewInputRecord(upperCaseString);
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("kafka", 2L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("kafka", 3L)));
    }
}