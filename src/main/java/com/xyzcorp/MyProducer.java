package com.xyzcorp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyProducer {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG,
                "all");
        properties.put(ProducerConfig.RETRIES_CONFIG,
                "10");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                "400");

        KafkaProducer<String, Integer> producer =
                new KafkaProducer<>(properties);

        String countryString =
                "AK,AL,AZ,AR,CA,CO,CT,DE,FL,GA," +
                        "HI,ID,IL,IN,IA,KS,KY,LA,ME,MD," +
                        "MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ," +
                        "NM,NY,NC,ND,OH,OK,OR,PA,RI,SC," +
                        "SD,TN,TX,UT,VT,VA,WA,WV,WI,WY,";


        Random random = new Random();
        AtomicBoolean done = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            done.set(true);
            producer.close();
        }));

        while (!done.get()) {
            String[] countries = countryString.split(",");
            int index = random.nextInt(countries.length - 1);
            String country = countries[index];
            int amount = random.nextInt(99999) + 1;

            ProducerRecord<String, Integer> producerRecord =
                    new ProducerRecord<>("my_orders", country, amount);

            Future<RecordMetadata> future = producer.send(producerRecord);
            try {
                RecordMetadata metadata = future.get();
                System.out.format("key: %s\n", producerRecord.key());
                System.out.format("value: %s\n", producerRecord.value().toString());
                System.out.format("offset: %d\n", metadata.offset());
                System.out.format("partition: %d\n", metadata.partition());
                System.out.format("timestamp: %d\n", metadata.timestamp());
                System.out.format("topic: %s\n", metadata.topic());
                System.out.format("toString: %s\n", metadata.toString());
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Thread.sleep(random.nextInt(30000 - 5000) + 5000);
        }
    }
}
