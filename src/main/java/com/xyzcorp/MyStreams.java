package com.xyzcorp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class MyStreams {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
            "my_streams_app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.Integer().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> stream =
            builder.stream("my_orders"); //Key: State, Value: Amount

        //One branch
        stream.filter((key, value) -> key.equals("CA"))
              .to("california_state_orders");

        //Second branch
        stream.groupByKey()
              .count()
              .toStream()
              .peek((key, value) ->
                  System.out.printf("key: %s, value %d", key, value))
              .to("state_orders_count",
                  Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
