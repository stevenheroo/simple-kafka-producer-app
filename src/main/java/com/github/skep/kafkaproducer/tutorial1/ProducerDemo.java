package com.github.skep.kafkaproducer.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final String BOOTSTRAP_SERVER_PORT = "127.0.0.1:9092";

    public static void main(String[] args) {
        Logger LOG = LoggerFactory.getLogger(ProducerDemo.class);

        //create properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_PORT);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //create a record
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first-topic", "hello world3");

        //send data
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                LOG.info("Error while producing" + e.getLocalizedMessage());
            }
            else {
                LOG.info("\n-------------------" +
                        "\nMetadata Response\n"+
                        "------------------\n"+
                        "TOPIC: " + recordMetadata.topic() + "\n"+
                        "PARTITION: " + recordMetadata.partition()+ "\n"+
                        "OFFSET: " + recordMetadata.offset() + "\n" +
                        "TIMESTAMP: " + recordMetadata.timestamp()
                );
            }
        });
        producer.flush();
    }
}
