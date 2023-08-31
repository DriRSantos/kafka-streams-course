package com.udemy.kafka.streams.course;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class FavouriteColourTest {
    TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setUpTopologyTestDriver(){

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favColorTest");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        FavouriteColour favouriteColour = new FavouriteColour();
        Topology topology = favouriteColour.createTopology();
        testDriver = new TopologyTestDriver(topology, props);
    }
    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String topicName, String key, String value) {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(topicName, stringSerde.serializer(), stringSerde.serializer());
        inputTopic.pipeInput(key, value);
    }
    public TestOutputTopic<String, Long> outputTopic(){
        return testDriver.createOutputTopic("favorite-colours-output", stringSerde.deserializer(), longSerde.deserializer());
    }

    @Test
    public void makeSureCountsByColorsAreCorrect(){
        String[][] inputData = {
                {"stephane","blue"},
                {"john","green"},
                {"dri","blue"},
                {"stephane","red"},
                {"alice","red"},
                {"dri","green"}
        };

        for (String[] data : inputData) {
            String key = data[0];
            String value = data[1];
            pushNewInputRecord("user-keys-and-colours", key, value);
        }

        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("blue", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("green", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("blue", 2L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("blue", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("red", 1L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("red", 2L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("blue", 0L)));
        assertThat(outputTopic().readKeyValue(), equalTo(new KeyValue<>("green", 2L)));
   }

    @Test
    public void makeSureColorsBecomeLowercase(){
        String[][] upperCaseColors = {
                {"stephane","BLUE"},
                {"john","Green"},
                {"dri","Blue"},
                {"stephane","Red"},
                {"alice","RED"},
                {"dri","green"}
        };
        for (String[] data : upperCaseColors) {
            String key = data[0];
            String value = data[1];
            pushNewInputRecord("favorite-colours-input", key, value);
        }

        while (!outputTopic().isEmpty()) {
            KeyValue<String, Long> record = outputTopic().readKeyValue();
            String color = record.key;
            Long count = record.value;

            assertThat(color, allOf(is(notNullValue()), is(not(equalTo("")))));
            assertThat(color, equalTo(color.toLowerCase()));
        }
    }

}
