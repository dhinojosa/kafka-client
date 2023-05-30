package com.xyzcorp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;

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

        //windowing
        //Third branch
        stream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(3), Duration.ofMinutes(2)))
            .aggregate(() -> 0L,
                (key1, value1, aggregate) -> value1 + aggregate)
            .toStream()
            .map((key, value) -> new KeyValue<>(key.window().endTime().toEpochMilli(), value))
            .to("windowed_time_topic", Produced.with(Serdes.Long(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
            private int currentFailureCount;
            final int maxFailures = 10;
            final long maxTimeIntervalMillis = 1000;
            private Instant previousErrorTime;

            @Override
            public StreamThreadExceptionResponse handle(Throwable throwable) {
                currentFailureCount++;
                Instant currentErrorTime = Instant.now();

                if (previousErrorTime == null) {
                    previousErrorTime = currentErrorTime;
                }

                long millisBetweenFailure = ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime);

                if (currentFailureCount >= maxFailures) {
                    if (millisBetweenFailure <= maxTimeIntervalMillis) {
                        return SHUTDOWN_APPLICATION;
                    } else {
                        currentFailureCount = 0;
                        previousErrorTime = null;
                    }
                }
                return REPLACE_THREAD;
            }
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
