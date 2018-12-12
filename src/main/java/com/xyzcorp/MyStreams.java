package com.xyzcorp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
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
                builder.stream("my_orders");

        KStream<String, Integer> ca =
                stream.filter((key, value) -> key.endsWith("CA"));

        ca.peek((s, n) ->
                System.out.format("Filter msg: %s, %d\n", s, n))
          .to("priority_state_orders");

        stream.groupByKey()
          .count()
          .toStream()
          .peek((s, n) -> System.out.format("Group msg: %s, %d\n", s, n))
          .to("state_orders_count",
                  Produced.with(Serdes.String(), Serdes.Long()));

        Topology build = builder.build();

        KafkaStreams streams = new KafkaStreams(build, props);

        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
