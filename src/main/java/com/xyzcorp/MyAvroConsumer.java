package com.xyzcorp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class MyAvroConsumer {

    public static String collectionTopicPartitionToString
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
        String schemaRegistryURL = Optional.ofNullable(System.getenv(
            "SCHEMA_REGISTRY_URL")).orElse("http://localhost:8081");
        String groupId = Optional.ofNullable(System.getenv(
            "GROUP_ID")).orElse("my-avro-group");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.setProperty("schema.registry.url", schemaRegistryURL);
        properties.setProperty("specific.avro.reader", "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Order> consumer =
                new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("my-avro-orders"),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        System.out.println("Partition revoked:" +
                                collectionTopicPartitionToString(collection));
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        System.out.println("Partition assigned:" +
                                collectionTopicPartitionToString(collection));
                    }
                });

        AtomicBoolean done = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            done.set(true);
        }));

        while (!done.get()) {
            ConsumerRecords<String, Order> records =
                    consumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, Order> record : records) {
                System.out.format("offset: %d\n", record.offset());
                System.out.format("partition: %d\n", record.partition());
                System.out.format("timestamp: %d\n", record.timestamp());
                System.out.format("timeStampType: %s\n",
                        record.timestampType());
                System.out.format("topic: %s\n", record.topic());
                System.out.format("key: %s\n", record.key());
                System.out.format("value: %s\n", record.value());
            }
        }
        consumer.close();
    }
}


