package com.xyzcorp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class MyConsumer {

    //State, and in a consumer that's a lot of work
    //it is likely better use KafkaStreams or KSQLDB

    private static String collectionTopicPartitionToString
            (Collection<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                              .map(tp -> tp.topic() + " - " + tp.partition())
                              .collect(Collectors.joining(","));
    }

    @SuppressWarnings({"Duplicates"})
    public static void main(String[] args) {
        Properties properties = new Properties();
        String bootstrapServer = Optional.ofNullable(System.getenv(
            "BOOTSTRAP_SERVERS")).orElse("localhost:9092");
        String groupId = Optional.ofNullable(System.getenv(
            "GROUP_ID")).orElse("my_group");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 200);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("my-orders"),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        System.out.println("Partition revoked:" +
                                collectionTopicPartitionToString(collection));
                        consumer.commitAsync();
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        System.out.println("Partition assigned:" +
                                collectionTopicPartitionToString(collection));
                    }
                });

        AtomicBoolean done = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> done.set(true)));

        while (!done.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> record : records) {
                System.out.format("offset: %d\n", record.offset());
                System.out.format("partition: %d\n", record.partition());
                System.out.format("timestamp: %d\n", record.timestamp());
                System.out.format("timeStampType: %s\n",
                        record.timestampType());
                System.out.format("topic: %s\n", record.topic());
                System.out.format("key: %s\n", record.key());
                System.out.format("value: %s\n", record.value());
            }

            consumer.commitAsync((offsets, exception) -> {
//                offsets.
//                exception.
            });

            boolean isClosed = false;
            isClosed = isClosed(consumer);

            System.out.println(isClosed);
        }
        consumer.commitSync(); //Block
        consumer.close();

        isClosed(consumer);
    }

    private static boolean isClosed(KafkaConsumer<String, String> consumer) {
        try {
            var consumerClass = consumer.getClass();
            Field field = consumerClass.getDeclaredField("closed");
            field.setAccessible(true);
            return (Boolean) field.get(consumer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}


