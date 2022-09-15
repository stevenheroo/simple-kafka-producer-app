package com.github.skep.kafkaproducer.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    Logger LOG = LoggerFactory.getLogger(ProducerDemo.class);

    public ConsumerDemo(Properties prp) {
        subscribe(prp);
    }

    public void subscribe(Properties prp) {
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prp);
        consumer.subscribe(Collections.singletonList("first-topic"));
        //subscribe consumer to topics
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            for (ConsumerRecord<String, String> record : records) {
                LOG.info("\n-------------------" +
                        "\nMetadata Response\n" +
                        "------------------\n" +
                        "TOPIC: " + record.topic() + "\n" +
                        "VALUE: " + record.value() + "\n" +
                        "PARTITION: " + record.partition() + "\n" +
                        "OFFSET: " + record.offset() + "\n" +
                        "TIMESTAMP: " + record.timestamp()
                );
            }

    }


}
