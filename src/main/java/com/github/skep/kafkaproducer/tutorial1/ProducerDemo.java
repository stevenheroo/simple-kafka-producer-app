package com.github.skep.kafkaproducer.tutorial1;

import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    private static final String BOOTSTRAP_SERVER_PORT = "127.0.0.1:9092";

    public static void main(String[] args) {
        //create properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_PORT);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //create a record
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first-topic", "hello world2");

        //send data
        producer.send(record);
        producer.close();
    }
}
